# Pyred: A Simple, Redis-like In-Memory Key-Value Store in Python (Experimental)

**Pyred** is a lightweight, file-backed, in-memory key-value data store written entirely in Python. It's designed for simplicity and ease of use, offering both synchronous and asynchronous clients that automatically manage their own server process. Inspired by Redis, Pyred supports common data structures like strings, lists, hashes, and sets, making it a handy tool for local development, caching, and small-scale applications.

> **Note:** This is an experimental project. It's great for learning and local use cases but is not recommended for production environments.

## Key Features

-   **Redis-like API:** Familiar commands like `set`, `get`, `lpush`, `hset`, etc.
-   **Sync and Async Clients:** Use `pyred.Store` for synchronous operations or `pyred.AsyncStore` for `asyncio`-based applications.
-   **Automatic Server Management:** The client automatically starts the server in the background on first use. No need to manage a separate server process.
-   **Data Persistence:** The store is automatically backed up to disk (`~/.pyred/store.bak`) and reloaded on startup.
-   **Multiple Data Types:** Supports strings, lists, hashes, and sets.
-   **Zero Configuration:** Works out of the box with sensible defaults.

## Installation

Currently, Pyred is not available on PyPI. To use it, clone the repository and add it to your Python path.

```bash
git clone https://github.com/Jamescog/pyred.git
cd pyred
```

You can then import it in your project.

## Usage

Pyred is incredibly simple to get started with. Just import and instantiate the client.

### Synchronous Client (`pyred.Store`)

The synchronous client is perfect for standard Python scripts and web frameworks like Flask or Django.

```python
from pyred import Store

# The server starts automatically in the background on first instantiation
db = Store()

# --- String Operations ---
db.set("user:1", "Alice")
print(db.get("user:1"))  # Output: Alice

db.set("counter", 100)
db.incr("counter")
print(db.get("counter"))  # Output: 101

db.decrby("counter", 10)
print(db.get("counter"))  # Output: 91

# --- List Operations ---
db.lpush("tasks", "write code")
db.rpush("tasks", "debug code")
print(db.lrange("tasks", 0, -1))  # Output: ['write code', 'debug code']
print(db.lpop("tasks"))          # Output: 'write code'

# --- Hash Operations ---
db.hset("user:profile", "name", "Bob")
db.hset("user:profile", "age", 30)
print(db.hgetall("user:profile")) # Output: {'name': 'Bob', 'age': 30}
print(db.hget("user:profile", "name")) # Output: 'Bob'

# --- Set Operations ---
db.sadd("tags", "python")
db.sadd("tags", "database")
db.sadd("tags", "python") # Ignored, as it's a duplicate
print(db.smembers("tags")) # Output: ['python', 'database'] (order may vary)
print(db.sismember("tags", "java")) # Output: 0 (False)
```

### Asynchronous Client (`pyred.AsyncStore`)

The asynchronous client is designed for use with `asyncio` and frameworks like FastAPI.

```python
import asyncio
from pyred import AsyncStore

async def main():
    # The server starts automatically in the background
    db = AsyncStore()

    # --- String Operations ---
    await db.aset("user:1", "Alice")
    user = await db.aget("user:1")
    print(user)  # Output: Alice

    # --- List Operations ---
    await db.alpush("tasks", "write async code")
    tasks = await db.alrange("tasks", 0, -1)
    print(tasks)  # Output: ['write async code']

    # --- Hash Operations ---
    await db.ahset("user:profile", "email", "alice@example.com")
    email = await db.ahget("user:profile", "email")
    print(email) # Output: 'alice@example.com'

    # Close the connection when done (optional, but good practice)
    await db.aclose()

if __name__ == "__main__":
    asyncio.run(main())
```

## How It Works

Pyred consists of a lightweight TCP server and a client. When you first instantiate a client (`Store` or `AsyncStore`), it attempts to connect to the server on `localhost:9876`. If the connection fails, it assumes the server isn't running and launches it in a background thread. This server process is automatically terminated when the main application exits.

All data is stored in memory for high performance and periodically saved to a backup file (`~/.pyred/store.bak`) to ensure persistence across sessions.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.