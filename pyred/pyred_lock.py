import os
import sqlite3

class PyredLock:
    """
    A reusable, file-based lock using SQLite exclusive transactions.
    """
    def __init__(self, path: str, timeout: float = 5.0):
        self._path = str(path)
        self._timeout = timeout
        self._conn = None
        self._locked = False

    def _ensure_connected(self):
        if self._conn is None:
            os.makedirs(os.path.dirname(self._path), exist_ok=True)
            self._conn = sqlite3.connect(self._path, timeout=self._timeout, isolation_level=None)
            self._conn.execute("PRAGMA journal_mode = WAL;")
            self._conn.execute("PRAGMA synchronous = NORMAL;")

    def acquire(self):
        """Acquires the lock, blocking up to the timeout."""
        if self._locked:
            return
        self._ensure_connected()
        try:
            self._conn.execute("BEGIN EXCLUSIVE;")
            self._locked = True
        except sqlite3.OperationalError as e:
            raise TimeoutError(f"Could not acquire lock on {self._path} within {self._timeout}s") from e

    def release(self):
        """Releases the lock."""
        if not self._locked:
            return
        self._conn.execute("COMMIT;")
        self._locked = False

    def close(self):
        """Closes the underlying database connection."""
        if self._conn:
            if self._locked:
                self._conn.execute("ROLLBACK;")
                self._locked = False
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        self.release()

    def __del__(self):
        self.close()