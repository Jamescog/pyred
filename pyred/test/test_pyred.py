# test_pyred.py

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


import pytest
import pytest_asyncio
import asyncio
import time
import threading
from pyred.server import start_server_background
from pyred.client import PyredSyncClient, PyredAsyncClient, PyredException

TEST_HOST = 'localhost'
TEST_PORT = 9877

@pytest.fixture(scope="session", autouse=True)
def start_server():
    """Starts the server once per test session."""
    start_server_background(host=TEST_HOST, port=TEST_PORT)

@pytest.fixture
def client():
    """Provides a synchronous client for each test."""
    c = PyredSyncClient(host=TEST_HOST, port=TEST_PORT)
    yield c
    c.close()

@pytest_asyncio.fixture
async def async_client():
    """Provides an asynchronous client for each test."""
    c = PyredAsyncClient(host=TEST_HOST, port=TEST_PORT)
    yield c
    try:
        await c.aclose()
    except RuntimeError as e:
        if "Event loop is closed" not in str(e):
            raise

@pytest.fixture(autouse=True)
def flush_db_before_each(client):
    """Ensures the database is clean before each test runs."""
    client.flushdb()


# --- Generic Command Tests ---

def test_ping(client):
    assert client.ping() == "pong"

@pytest.mark.asyncio
async def test_async_ping(async_client):
    assert await async_client.aping() == "pong"

def test_exists(client):
    assert client.exists("nonexistent") == 0
    client.set("mykey", "v")
    assert client.exists("mykey") == 1
    assert client.exists("nonexistent") == 0

@pytest.mark.asyncio
async def test_async_exists(async_client):
    assert await async_client.aexists("nonexistent") == 0
    await async_client.aset("mykey", "v")
    assert await async_client.aexists("mykey") == 1

def test_type(client):
    assert client.type("nonexistent") == "none"
    client.set("k_string", "v")
    assert client.type("k_string") == "string"
    client.lpush("k_list", "v")
    assert client.type("k_list") == "list"
    client.hset("k_hash", "f", "v")
    assert client.type("k_hash") == "hash"
    client.sadd("k_set", "v")
    assert client.type("k_set") == "set"

@pytest.mark.asyncio
async def test_async_type(async_client):
    assert await async_client.atype("nonexistent") == "none"
    await async_client.aset("k_string", "v")
    assert await async_client.atype("k_string") == "string"
    await async_client.alpush("k_list", "v")
    assert await async_client.atype("k_list") == "list"
    await async_client.ahset("k_hash", "f", "v")
    assert await async_client.atype("k_hash") == "hash"
    await async_client.asadd("k_set", "v")
    assert await async_client.atype("k_set") == "set"

def test_dbsize_and_flushdb(client):
    assert client.dbsize() == 0
    client.set("k1", "v")
    client.set("k2", "v")
    assert client.dbsize() == 2
    client.flushdb()
    assert client.dbsize() == 0
    assert client.get("k1") is None

@pytest.mark.asyncio
async def test_async_dbsize_and_flushdb(async_client):
    assert await async_client.adbsize() == 0
    await async_client.aset("k1", "v")
    await async_client.aset("k2", "v")
    assert await async_client.adbsize() == 2
    await async_client.aflushdb()
    assert await async_client.adbsize() == 0
    assert await async_client.aget("k1") is None

# --- String Command Tests ---

def test_set_get_delete(client):
    assert client.set("key1", "value1") == 1
    assert client.get("key1") == "value1"
    assert client.delete("key1") == 1
    assert client.get("key1") is None
    assert client.delete("nonexistent") == 0

@pytest.mark.asyncio
async def test_async_set_get_delete(async_client):
    assert await async_client.aset("akey1", "avalue1") == 1
    assert await async_client.aget("akey1") == "avalue1"
    assert await async_client.adelete("akey1") == 1
    assert await async_client.aget("akey1") is None
    assert await async_client.adelete("nonexistent") == 0

def test_incr_decr(client):
    assert client.incr("counter") == 1
    assert client.incr("counter") == 2
    assert client.incrby("counter", 5) == 7
    assert client.get("counter") == 7
    assert client.decr("counter") == 6
    assert client.decrby("counter", 3) == 3
    assert client.get("counter") == 3
    with pytest.raises(PyredException, match="Value is not an integer"):
        client.set("not_a_number", "hello")
        client.incr("not_a_number")

@pytest.mark.asyncio
async def test_async_incr_decr(async_client):
    assert await async_client.aincr("counter") == 1
    assert await async_client.aincr("counter") == 2
    assert await async_client.aincrby("counter", 5) == 7
    assert await async_client.aget("counter") == 7
    assert await async_client.adecr("counter") == 6
    assert await async_client.adecrby("counter", 3) == 3
    assert await async_client.aget("counter") == 3
    with pytest.raises(PyredException, match="Value is not an integer"):
        await async_client.aset("not_a_number", "hello")
        await async_client.aincr("not_a_number")

# --- List Command Tests ---

def test_list_push_pop_len(client):
    assert client.lpush("mylist", "a") == 1
    assert client.rpush("mylist", "b") == 2
    assert client.lpush("mylist", "c") == 3 # mylist is now [c, a, b]
    assert client.llen("mylist") == 3
    assert client.lpop("mylist") == "c"
    assert client.rpop("mylist") == "b"
    assert client.llen("mylist") == 1
    assert client.get("mylist") == ["a"]
    assert client.rpop("mylist") == "a"
    assert client.llen("mylist") == 0
    assert client.lpop("mylist") is None # Pop from empty list

@pytest.mark.asyncio
async def test_async_list_push_pop_len(async_client):
    assert await async_client.alpush("mylist", "a") == 1
    assert await async_client.arpush("mylist", "b") == 2
    assert await async_client.alpush("mylist", "c") == 3 # mylist is now [c, a, b]
    assert await async_client.allen("mylist") == 3
    assert await async_client.alpop("mylist") == "c"
    assert await async_client.arpop("mylist") == "b"
    assert await async_client.allen("mylist") == 1
    assert await async_client.aget("mylist") == ["a"]

def test_list_range_index(client):
    # ** FIX **: Removed the incorrect multi-argument call.
    # We add items one-by-one to match the current implementation.
    client.rpush("rlist", "one")
    client.rpush("rlist", "two")
    client.rpush("rlist", "three")
    client.rpush("rlist", "four")
    client.rpush("rlist", "five")
    assert client.lrange("rlist", 0, -1) == ["one", "two", "three", "four", "five"]
    assert client.lrange("rlist", 1, 3) == ["two", "three", "four"]
    assert client.lindex("rlist", 0) == "one"
    assert client.lindex("rlist", -1) == "five"
    assert client.lindex("rlist", 100) is None
    assert client.lrange("nonexistent", 0, -1) == []

@pytest.mark.asyncio
async def test_async_list_range_index(async_client):
    await async_client.arpush("rlist", "one")
    await async_client.arpush("rlist", "two")
    await async_client.arpush("rlist", "three")
    assert await async_client.alrange("rlist", 0, -1) == ["one", "two", "three"]
    assert await async_client.alindex("rlist", 1) == "two"
    assert await async_client.alindex("rlist", -1) == "three"
    assert await async_client.alindex("rlist", 100) is None

# --- Hash Command Tests ---

def test_hash_operations_all(client):
    client.hset("hash1", "field1", "val1")
    client.hset("hash1", "field2", "val2")
    assert client.hget("hash1", "field1") == "val1"
    assert client.hgetall("hash1") == {"field1": "val1", "field2": "val2"}
    assert client.hlen("hash1") == 2
    assert sorted(client.hkeys("hash1")) == ["field1", "field2"]
    assert sorted(client.hvals("hash1")) == ["val1", "val2"]
    assert client.hdel("hash1", "field1") == 1
    assert client.hlen("hash1") == 1

@pytest.mark.asyncio
async def test_async_hash_operations_all(async_client):
    await async_client.ahset("hash1", "field1", "val1")
    await async_client.ahset("hash1", "field2", "val2")
    assert await async_client.ahget("hash1", "field1") == "val1"
    assert await async_client.ahgetall("hash1") == {"field1": "val1", "field2": "val2"}
    assert await async_client.ahlen("hash1") == 2
    assert sorted(await async_client.ahkeys("hash1")) == ["field1", "field2"]
    assert sorted(await async_client.ahvals("hash1")) == ["val1", "val2"]

# --- Set Command Tests ---

def test_set_operations(client):
    assert client.sadd("myset", "a") == 1
    assert client.sadd("myset", "b") == 1
    assert client.sadd("myset", "a") == 0  # Duplicate
    assert client.scard("myset") == 2
    assert client.sismember("myset", "a") == 1
    assert client.sismember("myset", "c") == 0
    assert set(client.smembers("myset")) == {"a", "b"}
    assert client.srem("myset", "b") == 1
    assert client.srem("myset", "c") == 0
    assert client.scard("myset") == 1
    assert client.smembers("myset") == ["a"]

@pytest.mark.asyncio
async def test_async_set_operations(async_client):
    assert await async_client.asadd("myset", "a") == 1
    # ** FIX **: Changed `sadd` to `asadd` to use the async method.
    assert await async_client.asadd("myset", "b") == 1
    assert await async_client.asadd("myset", "a") == 0
    assert await async_client.ascard("myset") == 2
    assert await async_client.asismember("myset", "a") == 1
    assert set(await async_client.asmembers("myset")) == {"a", "b"}
    assert await async_client.asrem("myset", "a") == 1
    assert await async_client.ascard("myset") == 1

# --- Error and Edge Case Tests ---

def test_wrong_type_error(client):
    client.set("string_key", "i am a string")
    with pytest.raises(PyredException, match="WRONGTYPE"):
        client.hget("string_key", "field")
    with pytest.raises(PyredException, match="WRONGTYPE"):
        client.lpush("string_key", "v")
    with pytest.raises(PyredException, match="WRONGTYPE"):
        client.sadd("string_key", "v")

def test_empty_key_and_value(client):
    with pytest.raises(PyredException, match="Key and value are required"):
        client.set("", "")

def test_key_expiration(client):
    client.set("exp_key", "i will disappear", expire=1)
    assert client.get("exp_key") == "i will disappear"
    time.sleep(1.5)
    assert client.get("exp_key") is None

# --- Additional Tests ---
def test_set_expire(client):
    assert client.set("exp", "v", expire=1) == 1
    assert client.get("exp") == "v"
    time.sleep(1.2)
    assert client.get("exp") is None

def test_list_negative_indices(client):
    client.rpush("neglist", "a")
    client.rpush("neglist", "b")
    client.rpush("neglist", "c")
    assert client.lindex("neglist", -1) == "c"
    assert client.lindex("neglist", -2) == "b"
    assert client.lrange("neglist", -2, -1) == ["b", "c"]

@pytest.mark.asyncio
async def test_async_set_expire(async_client):
    assert await async_client.aset("exp", "v", expire=1) == 1
    assert await async_client.aget("exp") == "v"
    await asyncio.sleep(1.2)
    assert await async_client.aget("exp") is None

@pytest.mark.asyncio
async def test_async_list_negative_indices(async_client):
    await async_client.arpush("neglist", "a")
    await async_client.arpush("neglist", "b")
    await async_client.arpush("neglist", "c")
    assert await async_client.alindex("neglist", -1) == "c"
    assert await async_client.alindex("neglist", -2) == "b"
    assert await async_client.alrange("neglist", -2, -1) == ["b", "c"]

# --- TTL Tests ---

def test_ttl(client):
    # Nonexistent key
    assert client.ttl("nope") == -2
    # Key with no expiry
    client.set("k", "v")
    assert client.ttl("k") == -1
    # Key with expiry
    client.set("exp", "v", expire=2)
    ttl_val = client.ttl("exp")
    assert 0 < ttl_val <= 2
    # After expiry
    time.sleep(2.1)
    assert client.ttl("exp") == -2

@pytest.mark.asyncio
async def test_async_ttl(async_client):
    # Nonexistent key
    assert await async_client.attl("nope") == -2
    # Key with no expiry
    await async_client.aset("k", "v")
    assert await async_client.attl("k") == -1
    # Key with expiry
    await async_client.aset("exp", "v", expire=2)
    ttl_val = await async_client.attl("exp")
    assert 0 < ttl_val <= 2
    # After expiry
    await asyncio.sleep(2.1)
    assert await async_client.attl("exp") == -2

# --- Concurrency and Other Tests ---

def test_multiple_clients():
    results = {}

    def worker(client_id):
        client = PyredSyncClient(host=TEST_HOST, port=TEST_PORT)
        key = f"client_key_{client_id}"
        value = f"client_value_{client_id}"
        client.set(key, value)
        results[key] = client.get(key)
        client.close()

    threads = []
    num_clients = 10
    for i in range(num_clients):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    
    # Verify results using a single client to ensure state is consistent
    verifier_client = PyredSyncClient(host=TEST_HOST, port=TEST_PORT)
    assert verifier_client.dbsize() == num_clients
    for i in range(num_clients):
        key = f"client_key_{i}"
        value = f"client_value_{i}"
        assert results.get(key) == value
        assert verifier_client.get(key) == value
    verifier_client.close()