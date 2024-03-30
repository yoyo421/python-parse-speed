import dataclasses
from fastapi import FastAPI, Response
from fastapi.routing import APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import struct
import numpy as np
import base64
import pydantic
import msgspec
import orjson
from typing import Final, Literal, TypeVar

router = APIRouter()

@dataclasses.dataclass
class RequestTabularData:
    size: int
    seed: int
    fields: list[str]
    class_type: Literal['dataclass', 'pydantic', 'msgspec']

@dataclasses.dataclass
class ResponseTabularData_DATACLASS:
    data: dict[str, list[float] | str]
    # moking a non tabular data, response times 10
    request: list[dict]

    def get_fastapi_response(self) -> Response:
        return Response(orjson.dumps(dataclasses.asdict(self)), headers={"Content-Type": "application/json"})
    
class ResponseTabularData_PYDANTIC(pydantic.BaseModel):
    data: dict[str, list[float] | str]
    # moking a non tabular data, response times 10
    request: list[dict]

    def get_fastapi_response(self) -> Response:
        return Response(self.model_dump_json(), headers={"Content-Type": "application/json"})
    
class ResponseTabularData_MSGSPEC(msgspec.Struct):
    data: dict[str, list[float] | str]
    # moking a non tabular data, response times 10
    request: list[dict]

    def get_fastapi_response(self) -> Response:
        return Response(msgspec.json.encode(self), headers={"Content-Type": "application/json"})

C = TypeVar('C', ResponseTabularData_DATACLASS, ResponseTabularData_PYDANTIC, ResponseTabularData_MSGSPEC)

CLASS_TYPES: Final[dict[Literal['dataclass', 'pydantic', 'msgspec'], C]] = {
    'dataclass': ResponseTabularData_DATACLASS,
    'pydantic': ResponseTabularData_PYDANTIC,
    'msgspec': ResponseTabularData_MSGSPEC
}

def raw_factory(cls: C, data: dict[str, np.ndarray], requests: RequestTabularData) -> C:
    return cls(
        data={field: arr.tolist() for field, arr in data.items()}, 
        request=[dataclasses.asdict(requests)] * 10
    )

def numpy_base64_factory(cls: C, data: dict[str, np.ndarray], requests: RequestTabularData) -> C:
    return cls(
        data={field: base64.encodebytes(arr.tobytes()).decode('utf-8') for field, arr in data.items()}, 
        request=[dataclasses.asdict(requests)] * 10
    )

def raw_base64_factory(cls: C, data: dict[str, np.ndarray], requests: RequestTabularData) -> C:
    return cls(
        data={field: base64.encodebytes(struct.pack(f'{len(arr)}f', *arr.tolist())).decode('utf-8') for field, arr in data.items()}, 
        request=[dataclasses.asdict(requests)] * 10
    )

def get_random_data(size: int, seed: int, fields: list[str]) -> dict[str, np.ndarray]:
    random_state = np.random.RandomState(seed)
    data = {
        field: (random_state.rand(size) + random_state.randint(1_000_000, size=size)).astype(np.float32) for field in fields
    }
    return data

@router.post("/get-raw")
def get_raw_data(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return raw_factory(CLASS_TYPES[body.class_type], data, body).get_fastapi_response()

@router.post("/get-numpy-base64")
def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return numpy_base64_factory(CLASS_TYPES[body.class_type], data, body).get_fastapi_response()

@router.post("/get-raw-base64")
def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return raw_base64_factory(CLASS_TYPES[body.class_type], data, body).get_fastapi_response()

@router.post("/async-get-raw")
async def get_raw_data(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return raw_factory(CLASS_TYPES[body.class_type], data, body).get_fastapi_response()

@router.post("/async-get-numpy-base64")
async def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return numpy_base64_factory(CLASS_TYPES[body.class_type], data, body).get_fastapi_response()

@router.post("/async-get-raw-base64")
async def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return raw_base64_factory(CLASS_TYPES[body.class_type], data, body).get_fastapi_response()

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
