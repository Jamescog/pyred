# __init__.py

"""
Pyred: A Simple, Redis-like In-Memory Key-Value Store in Python.

This package provides client interfaces to interact with the Pyred server.
- `Store`: A synchronous client for standard Python applications.
- `AsyncStore`: An asynchronous client for use with asyncio.
"""

from .sync import Store
from .async_ import AsyncStore

__all__ = ['Store', 'AsyncStore']