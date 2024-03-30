import requests
import timeit
import pandas as pd
from aiohttp import ClientSession
import asyncio
import datetime

REPEAT = 5
RUN_FOR_NUMBERS = 20

def get_fields(size: int) -> list[str]:
    return [f"field{i + 1}" for i in range(0, size)]

def get_request_times(url: str, json: dict, headers: dict) -> list[float]:
    return timeit.repeat(lambda: requests.post(url, json=json, headers=headers).json(), number=RUN_FOR_NUMBERS, repeat=REPEAT)

async def get_request_times_async(url: str, json: dict, headers: dict) -> list[float]:
    res = []
    async with ClientSession() as session:
        for _ in range(REPEAT):
            start = datetime.datetime.now()
            _requests = [session.post(url, json=json, headers=headers) for _ in range(RUN_FOR_NUMBERS)]
            _results = await asyncio.gather(*_requests)
            await asyncio.gather(*[_result.json() for _result in _results])
            [_result.close() for _result in _results]
            end = datetime.datetime.now()
            res.append((end - start).total_seconds())
    return res

event_loop = asyncio.new_event_loop()
df = pd.DataFrame([], columns=['endpoint', 'size','field_length','samples', 'backend-async', 'frontend-async'])
for is_async in ['async-backend', 'sync-backend']:
    PREFIX = "async-" if is_async == 'async-backend' else ""
    for size in [100, 1000, 10000]:
        for fields in [get_fields(2), get_fields(5), get_fields(10)]:
            body = {"size": size, "seed": 42, "fields": fields}
            for endpoint in ["get-raw", "get-numpy-base64", "get-raw-base64"]:
                res1 = get_request_times(
                    f"http://localhost:8000/{PREFIX}{endpoint}", 
                    json=body, 
                    headers={"Content-Type": "application/json", "X-Profile-File": f"{PREFIX}{endpoint}-{size}-f{len(fields)}.prof"}
                )
                row1 = pd.Series(
                    [endpoint, size, len(fields), res1, is_async, 'sequential'],
                    index=df.columns
                )

                res2 = event_loop.run_until_complete(
                    get_request_times_async(
                        f"http://localhost:8000/{PREFIX}{endpoint}",
                        json=body,
                        headers={"Content-Type": "application/json", "X-Profile-File": f"{PREFIX}{endpoint}-{size}-f{len(fields)}.prof"}
                    )
                )

                row2 = pd.Series(
                    [endpoint, size, len(fields), res2, is_async, 'parallel'],
                    index=df.columns
                )

                df = pd.concat([df, row1.to_frame().T, row2.to_frame().T], ignore_index=True)


df = df.explode('samples', ignore_index=True)
df.rename(columns={'samples': 'time'}, inplace=True)
df.to_csv(f'server-test.csv', index=True, index_label='index')
