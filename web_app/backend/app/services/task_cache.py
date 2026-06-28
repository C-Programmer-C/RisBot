from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from app.config import settings

if TYPE_CHECKING:
    from app.services.pyrus_client import PyrusClient


@dataclass
class _CacheEntry:
    tasks: list[dict[str, Any]]
    expires_at: float


class TaskCache:
    def __init__(self, ttl_seconds: float) -> None:
        self._ttl = ttl_seconds
        self._store: dict[tuple[int, int, str], _CacheEntry] = {}
        self._lock = threading.Lock()

    def _key(self, year: int, month: int, product_ids: str) -> tuple[int, int, str]:
        return (year, month, product_ids)

    def get(self, year: int, month: int, product_ids: str) -> list[dict[str, Any]] | None:
        key = self._key(year, month, product_ids)
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if time.monotonic() >= entry.expires_at:
                del self._store[key]
                return None
            return entry.tasks

    def set(self, year: int, month: int, product_ids: str, tasks: list[dict[str, Any]]) -> None:
        key = self._key(year, month, product_ids)
        with self._lock:
            self._store[key] = _CacheEntry(
                tasks=tasks,
                expires_at=time.monotonic() + self._ttl,
            )

    def get_or_fetch(
        self,
        client: PyrusClient,
        year: int,
        month: int,
        product_ids: str,
    ) -> list[dict[str, Any]]:
        cached = self.get(year, month, product_ids)
        if cached is not None:
            return cached

        tasks = client.register_tasks(year, month, product_ids)
        self.set(year, month, product_ids, tasks)
        return tasks

    def clear_year(self, year: int) -> None:
        with self._lock:
            for key in [k for k in self._store if k[0] == year]:
                del self._store[key]


task_cache = TaskCache(ttl_seconds=settings.pyrus_cache_ttl)
