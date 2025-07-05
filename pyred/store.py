# store.py

import socket
import json
import asyncio
import logging
from typing import Any, Optional, Dict, List
from pyred.server import start_server_background

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9876

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PyredException(Exception):
    """Custom exception for Pyred client errors."""
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return f"PyredException: {self.message}"

class PyredClient:
    """
    A synchronous and asynchronous client for the Pyred key-value store.
    
    Manages a persistent connection to the server for high performance and
    automatically attempts to reconnect if the connection is lost.
    It is recommended to use this class as a context manager.
    """
    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
        self._host = host
        self._port = port
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._socket: Optional[socket.socket] = None
        self._sync_buffer = b""
        start_server_background(self._host, self._port)

    async def _ensure_async_connected(self):
        """Establishes an asyncio connection if one does not exist."""
        if not self._writer or self._writer.is_closing():
            await self.aclose() # Ensure old resources are cleaned up
            self._reader, self._writer = await asyncio.open_connection(self._host, self._port)

    def _ensure_sync_connected(self):
        """Establishes a socket connection if one does not exist."""
        if self._socket is None:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self._host, self._port))

    def _read_line_sync(self) -> bytes:
        """Reads a single newline-terminated line from the synchronous socket."""
        while b'\n' not in self._sync_buffer:
            data = self._socket.recv(4096)
            if not data:
                raise PyredException("Connection closed by server")
            self._sync_buffer += data
        line, self._sync_buffer = self._sync_buffer.split(b'\n', 1)
        return line

    def send_command_sync(self, command: dict) -> dict:
        """Sends a command using the persistent synchronous connection."""
        try:
            self._ensure_sync_connected()
            self._socket.sendall(json.dumps(command).encode() + b'\n')
            data = self._read_line_sync()
        except (ConnectionError, BrokenPipeError, OSError) as e:
            logger.error(f"Sync connection error: {e}. Attempting to reconnect on next call.")
            self.close() 
            raise PyredException(f"Connection lost: {e}")

        try:
            response = json.loads(data.decode())
            if response.get("status") == "error":
                raise PyredException(response.get("message", "Unknown error"))
            return response
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode server response: {data}")
            raise PyredException(f"Invalid server response: {e}")

    async def send_command_async(self, command: dict) -> dict:
        """Sends a command using the persistent asynchronous connection."""
        try:
            await self._ensure_async_connected()
            self._writer.write(json.dumps(command).encode() + b'\n')
            await self._writer.drain()
            data = await self._reader.readline()
            if not data:
                raise PyredException("Connection closed by server")
        except (ConnectionError, BrokenPipeError, OSError) as e:
            logger.error(f"Async connection error: {e}. Attempting to reconnect on next call.")
            await self.aclose()
            raise PyredException(f"Connection lost: {e}")

        try:
            response = json.loads(data.decode())
            if response.get("status") == "error":
                raise PyredException(response.get("message", "Unknown error"))
            return response
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode server response: {data}")
            raise PyredException(f"Invalid server response: {e}")

    def close(self):
        """Closes the synchronous connection."""
        if self._socket:
            self._socket.close()
        self._socket = None
        self._sync_buffer = b""

    async def aclose(self):
        """Closes the asynchronous connection."""
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
        self._writer = self._reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()

    # --- Synchronous Public API Methods ---

    # -- Generic Commands --
    def exists(self, key: str) -> int:
        return self.send_command_sync({"cmd": "exists", "key": key}).get("data", 0)

    def type(self, key: str) -> str:
        return self.send_command_sync({"cmd": "type", "key": key}).get("data", "none")

    def flushdb(self) -> str:
        return self.send_command_sync({"cmd": "flushdb"}).get("data", "")
        
    def dbsize(self) -> int:
        return self.send_command_sync({"cmd": "dbsize"}).get("data", 0)

    def delete(self, key: str) -> int:
        return self.send_command_sync({"cmd": "del", "key": key}).get("data", 0)

    def ping(self) -> str:
        return self.send_command_sync({"cmd": "ping"}).get("data", "")
        
    # -- String Commands --
    def set(self, key: str, value: Any, expire: int = 0) -> int:
        return self.send_command_sync({"cmd": "set", "key": key, "value": value, "expire": expire}).get("data", 0)

    def get(self, key: str) -> Optional[Any]:
        return self.send_command_sync({"cmd": "get", "key": key}).get("data")

    def incr(self, key: str) -> int:
        return self.send_command_sync({"cmd": "incr", "key": key}).get("data", 0)

    def incrby(self, key: str, amount: int) -> int:
        return self.send_command_sync({"cmd": "incrby", "key": key, "amount": amount}).get("data", 0)
    
    def decr(self, key: str) -> int:
        return self.send_command_sync({"cmd": "decr", "key": key}).get("data", 0)

    def decrby(self, key: str, amount: int) -> int:
        return self.send_command_sync({"cmd": "decrby", "key": key, "amount": amount}).get("data", 0)

    # -- List Commands --
    def lpush(self, key: str, value: Any) -> int:
        return self.send_command_sync({"cmd": "lpush", "key": key, "value": value}).get("data", 0)

    def rpush(self, key: str, value: Any) -> int:
        return self.send_command_sync({"cmd": "rpush", "key": key, "value": value}).get("data", 0)

    def lpop(self, key: str) -> Optional[Any]:
        return self.send_command_sync({"cmd": "lpop", "key": key}).get("data")

    def rpop(self, key: str) -> Optional[Any]:
        return self.send_command_sync({"cmd": "rpop", "key": key}).get("data")

    def lrange(self, key: str, start: int, end: int) -> List:
        return self.send_command_sync({"cmd": "lrange", "key": key, "start": start, "end": end}).get("data", [])

    def llen(self, key: str) -> int:
        return self.send_command_sync({"cmd": "llen", "key": key}).get("data", 0)

    def lindex(self, key: str, index: int) -> Optional[Any]:
        return self.send_command_sync({"cmd": "lindex", "key": key, "index": index}).get("data")

    # -- Hash Commands --
    def hset(self, key: str, field: str, value: Any) -> int:
        return self.send_command_sync({"cmd": "hset", "key": key, "field": field, "value": value}).get("data", 0)

    def hget(self, key: str, field: str) -> Optional[Any]:
        return self.send_command_sync({"cmd": "hget", "key": key, "field": field}).get("data")

    def hdel(self, key: str, field: str) -> int:
        return self.send_command_sync({"cmd": "hdel", "key": key, "field": field}).get("data", 0)

    def hgetall(self, key: str) -> Optional[Dict]:
        return self.send_command_sync({"cmd": "hgetall", "key": key}).get("data")
        
    def hlen(self, key: str) -> int:
        return self.send_command_sync({"cmd": "hlen", "key": key}).get("data", 0)

    def hkeys(self, key: str) -> List:
        return self.send_command_sync({"cmd": "hkeys", "key": key}).get("data", [])

    def hvals(self, key: str) -> List:
        return self.send_command_sync({"cmd": "hvals", "key": key}).get("data", [])

    # -- Set Commands --
    def sadd(self, key: str, member: Any) -> int:
        return self.send_command_sync({"cmd": "sadd", "key": key, "member": member}).get("data", 0)
    
    def srem(self, key: str, member: Any) -> int:
        return self.send_command_sync({"cmd": "srem", "key": key, "member": member}).get("data", 0)

    def sismember(self, key: str, member: Any) -> int:
        return self.send_command_sync({"cmd": "sismember", "key": key, "member": member}).get("data", 0)
        
    def smembers(self, key: str) -> List:
        return self.send_command_sync({"cmd": "smembers", "key": key}).get("data", [])
        
    def scard(self, key: str) -> int:
        return self.send_command_sync({"cmd": "scard", "key": key}).get("data", 0)

    # --- Asynchronous Public API Methods ---

    # -- Generic Commands --
    async def aexists(self, key: str) -> int:
        return (await self.send_command_async({"cmd": "exists", "key": key})).get("data", 0)

    async def atype(self, key: str) -> str:
        return (await self.send_command_async({"cmd": "type", "key": key})).get("data", "none")

    async def aflushdb(self) -> str:
        return (await self.send_command_async({"cmd": "flushdb"})).get("data", "")
        
    async def adbsize(self) -> int:
        return (await self.send_command_async({"cmd": "dbsize"})).get("data", 0)

    async def adelete(self, key: str) -> int:
        return (await self.send_command_async({"cmd": "del", "key": key})).get("data", 0)
    
    async def aping(self) -> str:
        return (await self.send_command_async({"cmd": "ping"})).get("data", "")

    # -- String Commands --
    async def aset(self, key: str, value: Any, expire: int = 0) -> int:
        return (await self.send_command_async({"cmd": "set", "key": key, "value": value, "expire": expire})).get("data", 0)

    async def aget(self, key: str) -> Optional[Any]:
        return (await self.send_command_async({"cmd": "get", "key": key})).get("data")
    
    async def aincr(self, key: str) -> int:
        return (await self.send_command_async({"cmd": "incr", "key": key})).get("data", 0)

    async def aincrby(self, key: str, amount: int) -> int:
        return (await self.send_command_async({"cmd": "incrby", "key": key, "amount": amount})).get("data", 0)

    async def adecr(self, key: str) -> int:
        return (await self.send_command_async({"cmd": "decr", "key": key})).get("data", 0)

    async def adecrby(self, key: str, amount: int) -> int:
        return (await self.send_command_async({"cmd": "decrby", "key": key, "amount": amount})).get("data", 0)

    # -- List Commands --
    async def alpush(self, key: str, value: Any) -> int:
        return (await self.send_command_async({"cmd": "lpush", "key": key, "value": value})).get("data", 0)

    async def arpush(self, key: str, value: Any) -> int:
        return (await self.send_command_async({"cmd": "rpush", "key": key, "value": value})).get("data", 0)

    async def alpop(self, key: str) -> Optional[Any]:
        return (await self.send_command_async({"cmd": "lpop", "key": key})).get("data")

    async def arpop(self, key: str) -> Optional[Any]:
        return (await self.send_command_async({"cmd": "rpop", "key": key})).get("data")

    async def alrange(self, key: str, start: int, end: int) -> List:
        return (await self.send_command_async({"cmd": "lrange", "key": key, "start": start, "end": end})).get("data", [])

    async def allen(self, key: str) -> int:
        return (await self.send_command_async({"cmd": "llen", "key": key})).get("data", 0)

    async def alindex(self, key: str, index: int) -> Optional[Any]:
        return (await self.send_command_async({"cmd": "lindex", "key": key, "index": index})).get("data")

    # -- Hash Commands --
    async def ahset(self, key: str, field: str, value: Any) -> int:
        return (await self.send_command_async({"cmd": "hset", "key": key, "field": field, "value": value})).get("data", 0)

    async def ahget(self, key: str, field: str) -> Optional[Any]:
        return (await self.send_command_async({"cmd": "hget", "key": key, "field": field})).get("data")

    async def ahdel(self, key: str, field: str) -> int:
        return (await self.send_command_async({"cmd": "hdel", "key": key, "field": field})).get("data", 0)

    async def ahgetall(self, key: str) -> Optional[Dict]:
        return (await self.send_command_async({"cmd": "hgetall", "key": key})).get("data")

    async def ahlen(self, key: str) -> int:
        return (await self.send_command_async({"cmd": "hlen", "key": key})).get("data", 0)

    async def ahkeys(self, key: str) -> List:
        return (await self.send_command_async({"cmd": "hkeys", "key": key})).get("data", [])

    async def ahvals(self, key: str) -> List:
        return (await self.send_command_async({"cmd": "hvals", "key": key})).get("data", [])

    # -- Set Commands --
    async def asadd(self, key: str, member: Any) -> int:
        return (await self.send_command_async({"cmd": "sadd", "key": key, "member": member})).get("data", 0)
    
    async def asrem(self, key: str, member: Any) -> int:
        return (await self.send_command_async({"cmd": "srem", "key": key, "member": member})).get("data", 0)

    async def asismember(self, key: str, member: Any) -> int:
        return (await self.send_command_async({"cmd": "sismember", "key": key, "member": member})).get("data", 0)
        
    async def asmembers(self, key: str) -> List:
        return (await self.send_command_async({"cmd": "smembers", "key": key})).get("data", [])
        
    async def ascard(self, key: str) -> int:
        return (await self.send_command_async({"cmd": "scard", "key": key})).get("data", 0)