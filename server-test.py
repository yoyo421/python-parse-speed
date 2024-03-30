import requests
import timeit
import pandas as pd
from aiohttp import ClientSession
import asyncio
import datetime
from tqdm import tqdm

REPEAT = 5
RUN_FOR_NUMBERS = 20

def get_fields(size: int) -> list[str]:
    return [f"field{i + 1}" for i in range(0, size)]

def get_request_times(url: str, json: dict, headers: dict) -> list[float]:
    return timeit.repeat(lambda: requests.post(url, json=json, headers=headers).content, number=RUN_FOR_NUMBERS, repeat=REPEAT)

async def get_request_times_async(url: str, json: dict, headers: dict) -> list[float]:
    res = []
    async with ClientSession() as session:
        for _ in range(REPEAT):
            start = datetime.datetime.now()
            _requests = [session.post(url, json=json, headers=headers) for _ in range(RUN_FOR_NUMBERS)]
            _results = await asyncio.gather(*_requests)
            await asyncio.gather(*[_result.content.read() for _result in _results])
            [_result.close() for _result in _results]
            end = datetime.datetime.now()
            res.append((end - start).total_seconds())
    return res

event_loop = asyncio.new_event_loop()
df = pd.DataFrame([], columns=['response_class_type', 'endpoint', 'size','field_length','samples', 'backend-async', 'frontend-async'])
for response_class_type in tqdm(['dataclass', 'pydantic', 'msgspec', 'binary'], desc="Response Class Type"):
    for is_async in tqdm(['async-backend', 'sync-backend'], desc="Async Type", leave=False):
        PREFIX = "async-" if is_async == 'async-backend' else ""
        for size in tqdm([100, 1000, 10000], desc="Size", leave=False):
            for fields in tqdm([get_fields(2), get_fields(5), get_fields(10)], desc="Fields", leave=False):
                body = {"size": size, "seed": 42, "fields": fields, "class_type": response_class_type}
                for endpoint in tqdm(["get-raw", "get-numpy-base64", "get-raw-base64"], desc="Endpoint", leave=False):
                    res1 = get_request_times(
                        f"http://localhost:8000/{PREFIX}{endpoint}", 
                        json=body, 
                        headers={"Content-Type": "application/json", "X-Profile-File": f"{PREFIX}{endpoint}-{size}-f{len(fields)}.prof"}
                    )
                    row1 = pd.Series(
                        [response_class_type, endpoint, size, len(fields), res1, is_async, 'sequential'],
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
                        [response_class_type, endpoint, size, len(fields), res2, is_async, 'parallel'],
                        index=df.columns
                    )

                    df = pd.concat([df, row1.to_frame().T, row2.to_frame().T], ignore_index=True)


df = df.explode('samples', ignore_index=True)
df.rename(columns={'samples': 'time'}, inplace=True)
df['time per field'] = df['time'] / df['field_length']
df.to_csv(f'server-test.csv', index=True, index_label='index')
