# server.py

import asyncio
import json
import socket
import threading
from time import time, sleep
import heapq
import os
from pathlib import Path
import atexit


from .pyred_lock import PyredLock

_server_controller = None 


class SetEncoder(json.JSONEncoder):
    """A custom JSON encoder that converts sets to a serializable format."""
    def default(self, obj):
        if isinstance(obj, set):
            return {'_type': 'set', 'value': list(obj)}
        return json.JSONEncoder.default(self, obj)

def set_decoder(obj):
    """A JSON object hook that decodes our custom set format back to a set."""
    if '_type' in obj and obj['_type'] == 'set':
        return set(obj['value'])
    return obj

class Store:
    """
    An asynchronous, in-memory key-value store server.

    This class manages the server state, including data storage, key expirations,
    and handling of client connections. It also supports periodic backup to disk.
    """
    def __init__(self, backup_file: str = 'store.bak', backup_interval: int = 60):
        """Initializes the data stores, expiration management, and backup system."""
        self.data = {}
        self.expires = {}
        self.hq = []

        # Backup configuration
        self.backup_file = Path(backup_file) if backup_file else None
        self.lock_file = 'proc.lock'
        self.backup_interval = backup_interval
        self._backup_lock = asyncio.Lock()
        self.process_lock = PyredLock(self.lock_file, timeout=5.0)

        if self.backup_file:
            self._load_from_disk()

    def _load_from_disk(self):
        """Loads server state from the backup file if it exists."""
        if not self.backup_file or not self.backup_file.exists():
            return
        
        print(f"Loading data from {self.backup_file}...")
        try:
            with open(self.backup_file, 'r', encoding='utf-8') as f:
                backup_data = json.load(f, object_hook=set_decoder)
            
            self.data = backup_data.get('data', {})
            self.expires = backup_data.get('expires', {})
            
            now = time()
            self.hq = []
            keys_to_expire = []
            for key, expt in self.expires.items():
                if expt > now:
                    heapq.heappush(self.hq, (expt, key))
                else:
                    keys_to_expire.append(key)
            
            for key in keys_to_expire:
                self._expire_key(key)
            
            print(f"Loaded {len(self.data)} keys.")

        except (json.JSONDecodeError, IOError) as e:
            print(f"Error loading backup file {self.backup_file}: {e}. Starting fresh.")
            self.data, self.expires, self.hq = {}, {}, []

    # Method to save data to disk
    async def _save_to_disk(self):
        """Atomically saves the current server state to the backup file."""
        if not self.backup_file:
            return

        async with self._backup_lock:
            # Create a serializable snapshot of the current state
            backup_data = {'data': self.data, 'expires': self.expires}
            temp_file = self.backup_file.with_suffix('.tmp')

            def do_write():
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, cls=SetEncoder)
                print(f"Saving data to {self.backup_file}...")
                os.rename(temp_file, self.backup_file)

            try:
                try:
                    # Attempt to run file I/O in background
                    await asyncio.to_thread(do_write)
                except RuntimeError as e:
                    if "cannot schedule new futures after shutdown" in str(e):
                        print("[WARN] Event loop shut down, saving data synchronously")
                        do_write()
                    else:
                        raise
            except Exception as e:
                print(f"Error during backup: {e}")
                if temp_file.exists():
                    os.remove(temp_file)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        Handles a single client connection, processing newline-delimited JSON commands.
        
        The connection is kept alive until the client disconnects or an empty
        message is received.
        """
        while True:
            data = await reader.readline()
            if not data:
                break
            try:
                command = json.loads(data.decode())
                response = await self.process(command)
            except json.JSONDecodeError:
                response = {"status": "error", "message": "Invalid JSON format"}
            
            writer.write(json.dumps(response).encode() + b'\n')
            await writer.drain()
            
        writer.close()
        await writer.wait_closed()

    async def process(self, command: dict):
        """
        Parses a command and dispatches it to the appropriate handler.

        Args:
            command: A dictionary representing the parsed JSON command.

        Returns:
            A dictionary with the result of the command execution.
        """
        key = command.get("key")
        if key and key in self.expires and self.expires[key] < time():
            self._expire_key(key)

        cmd = command.get("cmd")
        handler = getattr(self, f"handle_{cmd}", None)
        if handler and callable(handler):
            return await handler(command)
        else:
            return {"status": "error", "message": f"Unknown command: {cmd}"}

    def _expire_key(self, key):
        """Internal method to expire a key."""
        if key in self.data:
            del self.data[key]
        if key in self.expires:
            del self.expires[key]
        # Note: We don't remove from the heap here for simplicity.
        # The background task will handle the stale entry.
        
    async def background_cleanup(self):
        """
        Periodically cleans up expired keys in the background.

        This task efficiently sleeps until the next key is due to expire by using
        a min-heap for expiration timestamps.
        """
        while True:
            if self.hq:
                expt, key = self.hq[0]
                now = time()
                
                if expt <= now:
                    heapq.heappop(self.hq)
                    if self.expires.get(key) == expt:
                        self._expire_key(key)
                    continue

                sleep_duration = max(0, expt - now)
                await asyncio.sleep(sleep_duration)
            else:
                await asyncio.sleep(5.0)

    # Background task for periodic backups
    async def background_backup(self):
        """Periodically saves the database to disk in the background."""
        if not self.backup_file or self.backup_interval <= 0:
            return 

        while True:
            await asyncio.sleep(self.backup_interval)
            await self._save_to_disk()

    # --- GENERIC COMMANDS ---

    async def handle_exists(self, data):
        """Handles the 'exists' command to check if a key exists."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        return {"status": "ok", "data": int(key in self.data)}

    async def handle_type(self, data):
        """Handles the 'type' command to get the value type of a key."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": "none"}
        if isinstance(value, dict):
            return {"status": "ok", "data": "hash"}
        if isinstance(value, list):
            return {"status": "ok", "data": "list"}
        if isinstance(value, set):
            return {"status": "ok", "data": "set"}
        return {"status": "ok", "data": "string"}

    async def handle_flushdb(self, data):
        """Handles the 'flushdb' command to delete all keys from memory."""
        self.data.clear()
        self.expires.clear()
        self.hq = []
        return {"status": "ok", "data": "OK"}
        
    async def handle_dbsize(self, data):
        """Handles the 'dbsize' command to get the number of keys."""
        return {"status": "ok", "data": len(self.data)}

    # --- STRING COMMANDS ---

    async def handle_set(self, data):
        """Handles the 'set' command to store a key-value pair."""
        key, value = data.get("key"), data.get("value")
        expire = data.get("expire", 0)
        if not key or value is None:
            return {"status": "error", "message": "Key and value are required"}
        self.data[key] = value
        if expire > 0:
            expt = time() + expire
            self.expires[key] = expt
            heapq.heappush(self.hq, (expt, key))
        else:
            self.expires.pop(key, None)
        return {"status": "ok", "data": 1}

    async def handle_get(self, data):
        """Handles the 'get' command to retrieve a value by key."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        return {"status": "ok", "data": self.data.get(key)}

    async def handle_del(self, data):
        """Handles the 'del' command to delete a key."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        existed = key in self.data
        self.data.pop(key, None)
        self.expires.pop(key, None)
        return {"status": "ok", "data": int(existed)}

    async def handle_incr(self, data):
        """Handles the 'incr' command to increment an integer value by 1."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key, 0)
        if isinstance(value, str):
            try:
                value = int(value)
            except ValueError:
                return {"status": "error", "message": "Value is not an integer or out of range"}
        elif not isinstance(value, int):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        value += 1
        self.data[key] = value
        return {"status": "ok", "data": value}

    async def handle_incrby(self, data):
        """Handles the 'incrby' command to increment an integer value by a given amount."""
        key = data.get("key")
        amount = data.get("amount")
        if not key or amount is None:
            return {"status": "error", "message": "Key and amount are required"}
        try:
            amount = int(amount)
        except (ValueError, TypeError):
            return {"status": "error", "message": "Amount must be an integer"}
        value = self.data.get(key, 0)
        if isinstance(value, str):
            try:
                value = int(value)
            except ValueError:
                return {"status": "error", "message": "Value is not an integer or out of range"}
        elif not isinstance(value, int):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        value += amount
        self.data[key] = value
        return {"status": "ok", "data": value}
        
    async def handle_decr(self, data):
        """Handles the 'decr' command to decrement an integer value by 1."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        
        data["amount"] = -1
        return await self.handle_incrby(data)

    async def handle_decrby(self, data):
        """Handles the 'decrby' command to decrement an integer value by a given amount."""
        key = data.get("key")
        amount = data.get("amount")
        if not key or amount is None:
            return {"status": "error", "message": "Key and amount are required"}
        try:
            amount = int(amount)
        except (ValueError, TypeError):
            return {"status": "error", "message": "Amount must be an integer"}

        data["amount"] = -amount
        return await self.handle_incrby(data)

    # --- HASH COMMANDS ---

    async def handle_hset(self, data):
        """Handles the 'hset' command for setting a field in a hash."""
        key, field, value = data.get("key"), data.get("field"), data.get("value")
        if not key or field is None or value is None:
            return {"status": "error", "message": "Key, field, and value are required"}
        if key not in self.data:
            self.data[key] = {}
        if not isinstance(self.data.get(key), dict):
            return {"status": "error", "message": f"WRONGTYPE Operation against a key holding the wrong kind of value"}
        self.data[key][field] = value
        return {"status": "ok", "data": 1}

    async def handle_hget(self, data):
        """Handles the 'hget' command for getting a field from a hash."""
        key, field = data.get("key"), data.get("field")
        if not key or field is None:
            return {"status": "error", "message": "Key and field are required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": None}
        if not isinstance(value, dict):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        return {"status": "ok", "data": value.get(field)}

    async def handle_hdel(self, data):
        """Handles the 'hdel' command for deleting a field from a hash."""
        key, field = data.get("key"), data.get("field")
        if not key or field is None:
            return {"status": "error", "message": "Key and field are required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": 0}
        if not isinstance(value, dict):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        
        existed = field in value
        if existed:
            del value[field]
        return {"status": "ok", "data": int(existed)}

    async def handle_hgetall(self, data):
        """Handles the 'hgetall' command for getting all fields in a hash."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": {}}
        if not isinstance(value, dict):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        return {"status": "ok", "data": value}

    async def handle_hlen(self, data):
        """Handles the 'hlen' command to get the number of fields in a hash."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": 0}
        if not isinstance(value, dict):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        return {"status": "ok", "data": len(value)}

    async def handle_hkeys(self, data):
        """Handles the 'hkeys' command to get all field names in a hash."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": []}
        if not isinstance(value, dict):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        return {"status": "ok", "data": list(value.keys())}

    async def handle_hvals(self, data):
        """Handles the 'hvals' command to get all values in a hash."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": []}
        if not isinstance(value, dict):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        return {"status": "ok", "data": list(value.values())}

    # --- LIST COMMANDS ---

    async def handle_lpush(self, data):
        """Handles the 'lpush' command to push a value to the left of a list."""
        key, value = data.get("key"), data.get("value")
        if not key or value is None:
            return {"status": "error", "message": "Key and value are required"}
        if key not in self.data:
            self.data[key] = []
        if not isinstance(self.data.get(key), list):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        self.data[key].insert(0, value)
        return {"status": "ok", "data": len(self.data[key])}

    async def handle_rpush(self, data):
        """Handles the 'rpush' command to push a value to the right of a list."""
        key, value = data.get("key"), data.get("value")
        if not key or value is None:
            return {"status": "error", "message": "Key and value are required"}
        if key not in self.data:
            self.data[key] = []
        if not isinstance(self.data.get(key), list):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        self.data[key].append(value)
        return {"status": "ok", "data": len(self.data[key])}

    async def handle_lpop(self, data):
        """Handles the 'lpop' command to pop a value from the left of a list."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": None}
        if not isinstance(value, list):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        if not value:
            return {"status": "ok", "data": None}
        result = value.pop(0)
        return {"status": "ok", "data": result}

    async def handle_rpop(self, data):
        """Handles the 'rpop' command to pop a value from the right of a list."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": None}
        if not isinstance(value, list):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        if not value:
            return {"status": "ok", "data": None}
        result = value.pop()
        return {"status": "ok", "data": result}

    async def handle_lrange(self, data):
        """Handles the 'lrange' command to get a range of elements from a list."""
        key = data.get("key")
        start = data.get("start", 0)
        end = data.get("end", -1)
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": []}
        if not isinstance(value, list):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        list_len = len(value)
        if start < 0:
            start += list_len
        if end < 0:
            end += list_len
        if start < 0:
            start = 0
        if end >= list_len:
            end = list_len - 1
        if start > end:
            return {"status": "ok", "data": []}
        return {"status": "ok", "data": value[start:end+1]}

    async def handle_llen(self, data):
        """Handles the 'llen' command to get the length of a list."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": 0}
        if not isinstance(value, list):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        return {"status": "ok", "data": len(value)}

    async def handle_lindex(self, data):
        """Handles the 'lindex' command to get an element from a list by its index."""
        key = data.get("key")
        index = data.get("index")
        if not key or index is None:
            return {"status": "error", "message": "Key and index are required"}
        try:
            index = int(index)
        except (ValueError, TypeError):
            return {"status": "error", "message": "Index must be an integer"}
        
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": None}
        if not isinstance(value, list):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        
        try:
            return {"status": "ok", "data": value[index]}
        except IndexError:
            return {"status": "ok", "data": None}

    # --- SET COMMANDS ---

    async def handle_sadd(self, data):
        """Handles the 'sadd' command to add a member to a set."""
        key, member = data.get("key"), data.get("member")
        if not key or member is None:
            return {"status": "error", "message": "Key and member are required"}
        if key not in self.data:
            self.data[key] = set()
        
        value = self.data.get(key)
        if not isinstance(value, set):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        
        if member in value:
            return {"status": "ok", "data": 0}
        else:
            value.add(member)
            return {"status": "ok", "data": 1}

    async def handle_srem(self, data):
        """Handles the 'srem' command to remove a member from a set."""
        key, member = data.get("key"), data.get("member")
        if not key or member is None:
            return {"status": "error", "message": "Key and member are required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": 0}
        if not isinstance(value, set):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        
        if member in value:
            value.remove(member)
            return {"status": "ok", "data": 1}
        else:
            return {"status": "ok", "data": 0}

    async def handle_sismember(self, data):
        """Handles the 'sismember' command to check if a member is in a set."""
        key, member = data.get("key"), data.get("member")
        if not key or member is None:
            return {"status": "error", "message": "Key and member are required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": 0}
        if not isinstance(value, set):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        
        return {"status": "ok", "data": int(member in value)}

    async def handle_smembers(self, data):
        """Handles the 'smembers' command to get all members of a set."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": []}
        if not isinstance(value, set):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        
        return {"status": "ok", "data": list(value)}

    async def handle_scard(self, data):
        """Handles the 'scard' command to get the number of members in a set."""
        key = data.get("key")
        if not key:
            return {"status": "error", "message": "Key is required"}
        value = self.data.get(key)
        if value is None:
            return {"status": "ok", "data": 0}
        if not isinstance(value, set):
            return {"status": "error", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"}
        
        return {"status": "ok", "data": len(value)}

    # --- PING ---
    async def handle_ping(self, data):
        """Handles the 'ping' command to check server liveness."""
        return {"status": "ok", "data": "pong"}


def default_parent_dir():
    """Returns the default parent directory for the backup file."""
    return Path.home() / '.pyred'


async def start_and_listen(host: str = 'localhost', port: int = 9876, shutdown_event: asyncio.Event = None):
    """
    Initializes and starts the server.

    This function creates a Store instance, which automatically loads from a backup
    if one exists. It then starts background tasks for key expiration and periodic
    backups before listening for incoming connections.
    """
    store = Store(backup_file=default_parent_dir() / 'store.bak', backup_interval=60)

    asyncio.create_task(store.background_cleanup())
    asyncio.create_task(store.background_backup())

    server = await asyncio.start_server(store.handle_client, host, port)

    loop = asyncio.get_running_loop()

    sockets = server.sockets or []
    if sockets:
        bound = sockets[0].getsockname()
        print(f"Server started on {bound[0]}:{bound[1]}")

    
    if shutdown_event is None:
        shutdown_event = asyncio.Event()
    
    async def serve_and_shutdown():
        async with server:
            await shutdown_event.wait()
            print("Shutdown signal received.  Saving state and shutting down...")
            await store._save_to_disk()
            print("State saved. Shutting down server.")

    await serve_and_shutdown()


def is_port_open(host: str, port: int) -> bool:
    """Checks if a TCP port is open and listening on the given host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.settimeout(0.3)
            sock.connect((host, port))
            return True
        except (ConnectionRefusedError, OSError):
            return False

_server_thread = None

def wait_for_port(host: str, port: int, timeout: float = 2.0):
    """
    Waits for a specified duration until a TCP port becomes open.

    Args:
        host: The host to check.
        port: The port to check.
        timeout: The maximum time to wait in seconds.

    Raises:
        RuntimeError: If the port does not open within the timeout period.
    """
    start = time()
    while time() - start < timeout:
        if is_port_open(host, port):
            return
        sleep(0.05)
    raise RuntimeError(f"Timeout: server did not start on {host}:{port}")


def start_server_background(host: str = 'localhost', port: int = 9876):
    """
    Starts the server in a background thread if it's not already running
    on the system. This function is thread-safe and process-aware.
    """
    global _server_controller


    lock_path = default_parent_dir() / 'pyred.lock'
    lock = PyredLock(lock_path, timeout=5.0)

    with lock:
        if is_port_open(host, port):
            return

        if _server_controller and _server_controller.thread.is_alive():
            return

        print(f"No server found on {host}:{port}. Starting one in the background...")

        def run():
            asyncio.run(start_and_listen(host, port))

        _server_controller = ServerThreadController(host, port)
        _server_controller.start()



class ServerThreadController:
    def __init__(self, host='localhost', port=9876):
        self.host = host
        self.port = port
        self.shutdown_trigger = threading.Event()
        self.thread = threading.Thread(target=self._run_server, daemon=True)

    def _run_server(self):
        asyncio.run(self._async_entrypoint())

    async def _async_entrypoint(self):
        shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def wait_for_shutdown():
            self.shutdown_trigger.wait()
            loop.call_soon_threadsafe(shutdown_event.set)

        threading.Thread(target=wait_for_shutdown, daemon=True).start()
        await start_and_listen(self.host, self.port, shutdown_event)

    def start(self):
        if is_port_open(self.host, self.port):
            print(f"Server already running on {self.host}:{self.port}")
            return
        print(f"Starting server in background on {self.host}:{self.port}")
        self.thread.start()
        wait_for_port(self.host, self.port)

    def stop(self):
        print("Triggering shutdown...")
        self.shutdown_trigger.set()
        self.thread.join(timeout=5)
        print("Server stopped.")


atexit.register(lambda: _server_controller and _server_controller.stop())
