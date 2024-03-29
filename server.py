from fastapi import FastAPI, Response
from fastapi.responses import ORJSONResponse
from fastapi.routing import APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
import struct
import numpy as np
import base64

router = APIRouter()

class RequestTabularData(BaseModel):
    size: int
    seed: int
    fields: list[str]

class ResponseTabularData(BaseModel):
    data: dict[str, list[float] | bytes | str]
    # moking a non tabular data, response times 10
    request: list[RequestTabularData]

def get_random_data(size: int, seed: int, fields: list[str]) -> dict[str, np.ndarray]:
    random_state = np.random.RandomState(seed)
    data = {
        field: (random_state.rand(size) + random_state.randint(1_000_000, size=size)).astype(np.float32) for field in fields
    }
    return data

@router.post("/get-raw", response_model=ResponseTabularData, response_class=ORJSONResponse)
def get_raw_data(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData(
        data={field: arr.tolist() for field, arr in data.items()}, 
        request=[body] * 10
    )

@router.post("/get-numpy-base64", response_model=ResponseTabularData, response_class=ORJSONResponse)
def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData(
        data={field: base64.encodebytes(arr.tobytes()) for field, arr in data.items()}, 
        request=[body] * 10
    )

@router.post("/get-raw-base64", response_model=ResponseTabularData, response_class=ORJSONResponse)
def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData(
        data={field: base64.encodebytes(struct.pack(f'{len(arr)}f', *arr.tolist())) for field, arr in data.items()}, 
        request=[body] * 10
    )

@router.post("/async-get-raw", response_model=ResponseTabularData, response_class=ORJSONResponse)
async def get_raw_data(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData(
        data={field: arr.tolist() for field, arr in data.items()}, 
        request=[body] * 10
    )

@router.post("/async-get-numpy-base64", response_model=ResponseTabularData, response_class=ORJSONResponse)
async def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData(
        data={field: base64.encodebytes(arr.tobytes()) for field, arr in data.items()}, 
        request=[body] * 10
    )

@router.post("/async-get-raw-base64", response_model=ResponseTabularData, response_class=ORJSONResponse)
async def get_numpy_bytes(body: RequestTabularData):
    data = get_random_data(body.size, body.seed, body.fields)
    return ResponseTabularData(
        data={field: base64.encodebytes(struct.pack(f'{len(arr)}f', *arr.tolist())) for field, arr in data.items()}, 
        request=[body] * 10
    )

def create_app():
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
    return app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(create_app(), host="localhost", port=8000)