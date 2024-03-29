#%%
import struct
import numpy as np
import orjson
import gzip
import random
#%%
def size(obj):
    print(f"Size of {obj}")
    print(len(obj))
#%%
# Define a list of 10 numpy floating-point numbers
numbers = [random.randint(0,100_000_000) + random.random() for _ in range(1_000)]
# Convert the list of numbers to JSON
json_data = orjson.dumps(numbers)
# Gzip the JSON data
size(json_data)
size(gzip.compress(json_data))
size(np.array(numbers, dtype=np.float32).astype('uint8').tobytes())
size(gzip.compress(np.array(numbers, dtype=np.float32).astype('uint8').tobytes()))
size(struct.pack('!%sf' % len(numbers), *numbers))
size(gzip.compress(struct.pack('!%sf' % len(numbers), *numbers)))
# %% normal cast
%%timeit
a = orjson.dumps(numbers)
# %% normal cast
%%timeit
a = gzip.compress(orjson.dumps(numbers))
# %% numpy cast
%%timeit
a = np.array(numbers, dtype='<f').tobytes()
# %% numpy cast
%%timeit
a = gzip.compress(np.array(numbers, dtype='<f').astype('uint8').tobytes())
# %% little endian
%%timeit
a = struct.pack('%sf' % len(numbers), *numbers)
# %% little endian
%%timeit
a = gzip.compress(struct.pack('%sf' % len(numbers), *numbers))
# %% big endian
%%timeit
a = struct.pack('!%sf' % len(numbers), *numbers)
# %% big endian
%%timeit
a = gzip.compress(struct.pack('!%sf' % len(numbers), *numbers))
# %%
print([int(_) for _ in struct.pack('%sf' % len(numbers), *numbers)])
# %%
size(np.array(numbers, dtype='>f').tobytes())
size(struct.pack('%sf' % len(numbers), *numbers))
np.array(numbers, dtype='<f').tobytes() == struct.pack('%sf' % len(numbers), *numbers)
# %%
