import dataclasses
from fastapi import FastAPI, Response
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import struct
import numpy as np
import base64
import pydantic
import msgspec
import orjson
from typing import Any, Final, Generator, Iterable, Literal, TypeVar

router = APIRouter()

CLASS_TYPE = Literal['dataclass', 'pydantic', 'msgspec', 'binary']

@dataclasses.dataclass
class RequestTabularData:
    size: int
    seed: int
    fields: list[str]
    class_type: CLASS_TYPE

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

@dataclasses.dataclass()
class ResponseTabularData_BINARY:
    """
    Binary data:

    This binary data is a key-value store. Each of the pair has HEADER, so
    a HEADER for key and HEADER for value
    
    The body looks like this (big endian):

    3 bytes: magic number (0x41494d) "AIM"
    1 byte: version (0x01)
    
    HEADER (per key or value):
        - 1 byte: field type (1: key | utf8, 2: arr-float32)
        - 4 bytes: field length
            - for key | utf8: field name or length of the string, they are utf8 encoded both
            - for every arr-*: length of the array, the size for each item is known by field type
        - n bytes: field data (utf8 arr-*)
    LAST HEADER:
        - 1 byte: field type (0: json until EOF)
        - n bytes: json data (utf8, read until EOF)
        

    example:
        AIM\x01                                                                 # magic number and version
        \x01\x00\x00\x00\x06field1                                              # key: field1
        \x02\x00\x00\x00\x02                                                    # arr-float32 size 2
        ?\x80\x00\x00@\x00\x00\x00                                              # 1.0, 2.0
        \x01\x00\x00\x00\x06field2                                              # key: field2
        \x02\x00\x00\x00\x02                                                    # arr-float32 size 2
        @@\x00\x00@\x80\x00\x00                                                 # 3.0, 4.0
        \x01\x00\x00\x00\rempty_field\xd7\x92                                   # key: empty_fieldת
        0{"im_json": true, "hello": "javascript", "\xd7\xaa": "utf-8 support"}  # end of data
    """
    data: dict[str, list[float] | str]
    # moking a non tabular data, response times 10
    request: list[dict]

    _current_field: str = dataclasses.field(default=None, init=False)

    def decode_str(self, txt: str) -> bytes:
        txt_bytes = txt.encode('utf-8')
        return struct.pack('!BI', 1, len(txt_bytes)) + txt_bytes
    
    def decode_arr(self, size: int, arr: Iterable[Any]) -> bytes:
        if len(arr) == 0:
            return struct.pack('!BI', 2, 0)
        if isinstance(arr[0], float):
            return struct.pack('!BI', 2, size) + struct.pack(f'!{size}f', *arr)
        raise ValueError(f"Unknown type {type(arr[0])} for field {self._current_field} to parse")

    def get_fastapi_response(self) -> StreamingResponse:
        def generate_buffer() -> Generator[bytes, None, None]:
            yield b'AIM\x01' # magic number and version
            for field, value in self.data.items():
                self._current_field = field
                yield self.decode_str(field)
                if isinstance(value, str): # utf8
                    yield self.decode_str(value)
                elif isinstance(value, list): # arr-*
                    yield self.decode_arr(len(value), value)
                    
            orjson_data = orjson.dumps(self.request)
            yield b'0' + orjson_data # end of data
        # Response is faster for small data
        # return Response(b''.join(generate_buffer()), headers={"Content-Type": "application/octet-stream"})
        return StreamingResponse(generate_buffer(), headers={"Content-Type": "application/octet-stream"})

C = TypeVar(
    'C', 
    ResponseTabularData_DATACLASS, 
    ResponseTabularData_PYDANTIC, 
    ResponseTabularData_MSGSPEC,
    ResponseTabularData_BINARY
)

RESPONSE_CLASSES_BY_TYPE: Final[dict[CLASS_TYPE, C]] = {
    'dataclass': ResponseTabularData_DATACLASS,
    'pydantic': ResponseTabularData_PYDANTIC,
    'msgspec': ResponseTabularData_MSGSPEC,
    'binary': ResponseTabularData_BINARY
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
    return raw_factory(RESPONSE_CLASSES_BY_TYPE[body.class_type], data, body).get_fastapi_response()

@router.post("/get-numpy-base64")
def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return numpy_base64_factory(RESPONSE_CLASSES_BY_TYPE[body.class_type], data, body).get_fastapi_response()

@router.post("/get-raw-base64")
def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return raw_base64_factory(RESPONSE_CLASSES_BY_TYPE[body.class_type], data, body).get_fastapi_response()

@router.post("/async-get-raw")
async def get_raw_data(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return raw_factory(RESPONSE_CLASSES_BY_TYPE[body.class_type], data, body).get_fastapi_response()

@router.post("/async-get-numpy-base64")
async def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return numpy_base64_factory(RESPONSE_CLASSES_BY_TYPE[body.class_type], data, body).get_fastapi_response()

@router.post("/async-get-raw-base64")
async def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return raw_base64_factory(RESPONSE_CLASSES_BY_TYPE[body.class_type], data, body).get_fastapi_response()

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
