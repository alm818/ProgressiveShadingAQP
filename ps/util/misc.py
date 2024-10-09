import pickle, numpy, tdigest, configparser
from multiprocessing import shared_memory

def get_config():
    config = configparser.ConfigParser()
    config_file = open("config.txt", 'r')
    config.read_file(config_file)
    config_file.close()
    return config

def upload(shm_name, obj, create=False):
    if isinstance(obj, tdigest.TDigest):
        pickled_obj = pickle.dumps(obj.to_dict())
    else:
        pickled_obj = pickle.dumps(obj)
    size = len(pickled_obj)
    try:
        shm = shared_memory.SharedMemory(name=shm_name, create=create, size=size)
    except FileExistsError:
        shm = shared_memory.SharedMemory(name=shm_name, create=False)
        shm.close()
        shm.unlink()
        shm = shared_memory.SharedMemory(name=shm_name, create=create, size=size)
    assert size <= shm.size, f"shm_size {shm.size} while actual size {size}"
    shm.buf[:size] = pickled_obj
    shm.close()
    return shm, size

def divide_range(length, divide, start=0):
    interval = length // divide
    remainder = length % divide
    splits = [start] + [interval+1 for i in range(remainder)] + [interval for i in range(divide-remainder)]
    return numpy.cumsum(splits)