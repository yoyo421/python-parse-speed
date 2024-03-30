import dataclasses
from fastapi import FastAPI, Response
from fastapi.routing import APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import struct
import numpy as np
import base64
import orjson

router = APIRouter()

@dataclasses.dataclass
class RequestTabularData:
    size: int
    seed: int
    fields: list[str]

@dataclasses.dataclass
class ResponseTabularData:
    data: dict[str, list[float] | bytes | str]
    # moking a non tabular data, response times 10
    request: list[RequestTabularData]

    def get_fastapi_response(self) -> Response:
        return Response(orjson.dumps(dataclasses.asdict(self)), headers={"Content-Type": "application/json"})

    @staticmethod
    def raw(data: dict[str, np.ndarray], requests: RequestTabularData) -> 'ResponseTabularData':
        return ResponseTabularData(
            data={field: arr.tolist() for field, arr in data.items()}, 
            request=[requests] * 10
        )
    
    @staticmethod
    def numpy_base64(data: dict[str, np.ndarray], requests: RequestTabularData) -> 'ResponseTabularData':
        return ResponseTabularData(
            data={field: base64.encodebytes(arr.tobytes()).decode('utf-8') for field, arr in data.items()}, 
            request=[requests] * 10
        )
    
    @staticmethod
    def raw_base64(data: dict[str, np.ndarray], requests: RequestTabularData) -> 'ResponseTabularData':
        return ResponseTabularData(
            data={field: base64.encodebytes(struct.pack(f'{len(arr)}f', *arr.tolist())).decode('utf-8') for field, arr in data.items()}, 
            request=[requests] * 10
        )

def get_random_data(size: int, seed: int, fields: list[str]) -> dict[str, np.ndarray]:
    random_state = np.random.RandomState(seed)
    data = {
        field: (random_state.rand(size) + random_state.randint(1_000_000, size=size)).astype(np.float32) for field in fields
    }
    return data

@router.post("/get-raw", response_model=ResponseTabularData)
def get_raw_data(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData.raw(data, body).get_fastapi_response()

@router.post("/get-numpy-base64", response_model=ResponseTabularData)
def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData.numpy_base64(data, body).get_fastapi_response()

@router.post("/get-raw-base64", response_model=ResponseTabularData)
def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData.raw_base64(data, body).get_fastapi_response()

@router.post("/async-get-raw", response_model=ResponseTabularData)
async def get_raw_data(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData.raw(data, body).get_fastapi_response()

@router.post("/async-get-numpy-base64", response_model=ResponseTabularData)
async def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData.numpy_base64(data, body).get_fastapi_response()

@router.post("/async-get-raw-base64", response_model=ResponseTabularData)
async def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData.raw_base64(data, body).get_fastapi_response()

app = FastAPI()

@app.get("/heath")
async def root():
    return Response(content="OK", media_type="text/plain", status_code=200)

app.include_router(router)

# d = {'i': 0, 'main_stats': pstats.Stats()}

# @app.middleware("http")
# async def perfomance_middleware(request: Request, call_next):
#     profiler = cProfile.Profile()
#     profiler.enable()
#     response = await call_next(request)
#     profiler.disable()
#     d['main_stats'].add(profiler)
#     d['i']+=1
#     if d['i'] == 100:
#         d['main_stats'].dump_stats(request.headers.get("X-Profile-File", "profile.prof"))
#         d['main_stats'] = pstats.Stats()
#         d['i'] = 0
#     return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
