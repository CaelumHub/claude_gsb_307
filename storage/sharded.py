"""通用分片 JSON 存储引擎。

用于平台中「项目 / 测试用例 / 测试套件 / 缺陷 / 环境 / 定时计划 / 通知集成」
等实体型数据的持久化。每种实体一个独立目录，目录内是若干大小受控的分片文件
``shard_000001.json``，配合元数据 ``meta.json`` 记录分片数与自增 id。

与构建结果（按项目 + 构建二次分片）不同，这里的实体数量相对稳定、单条记录
较小，因此采用「每类实体一个分片集合」的粒度，见 :class:`ShardedStore`。
构建结果的存储见 :mod:`storage.buildstore`。

并发安全
--------
所有读-改-写都通过 :class:`~storage.lock.FileLock` 串行化：

- ``insert`` / ``insert_many`` / ``update`` / ``delete`` 使用排他锁，
  保护「读元数据 → 改分片 → 写回 → 更新元数据」这个完整序列；
- ``all`` / ``get`` / ``query`` 使用共享锁，读取期间不受并发写影响；
- 写文件一律走 :func:`~storage.atomic.atomic_write_json` 原子替换。
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Iterable, Iterator, Optional

from .atomic import atomic_write_json, read_json
from .lock import FileLock, lock_path_for


# 默认每个分片最多容纳的记录数
DEFAULT_SHARD_SIZE = 200

# 查询支持的比较操作符
_OPERATORS = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "in": lambda a, b: a in b,
    "not_in": lambda a, b: a not in b,
    "contains": lambda a, b: b in a if hasattr(a, "__contains__") else False,
    "startswith": lambda a, b: isinstance(a, str) and a.startswith(b),
    "gt": lambda a, b: a > b,
    "gte": lambda a, b: a >= b,
    "lt": lambda a, b: a < b,
    "lte": lambda a, b: a <= b,
}


def _extract(record: dict, dotted: str):
    """按 ``a.b.c`` 点路径取值，缺失返回 None。"""
    v = record
    for part in dotted.split("."):
        if not isinstance(v, dict):
            return None
        v = v.get(part)
    return v


def _sortable(value):
    """把任意值转成可比较的排序键。

    None 恒排最后；不同类型（int / str / dict…）之间按类型名分组，避免
    Python 直接比较 ``int < str`` 抛 ``TypeError``。这样即便记录里该字段
    有空值、类型混杂，排序也不会崩，且稳定不重不漏。
    """
    if value is None:
        return (1, "")
    return (0, type(value).__name__, value)


class ShardedStore:
    """单个实体类型的 JSON 分片存储。"""

    def __init__(self, root: str, name: str, shard_size: int = DEFAULT_SHARD_SIZE):
        self.name = name
        self.shard_size = max(1, int(shard_size))
        self.dir = os.path.join(root, name)
        self.meta_path = os.path.join(self.dir, "meta.json")
        os.makedirs(self.dir, exist_ok=True)
        self._ensure_meta()

    # -- 元数据 -----------------------------------------------------------
    def _ensure_meta(self) -> dict:
        if not os.path.exists(self.meta_path):
            atomic_write_json(self.meta_path, {
                "name": self.name,
                "shard_size": self.shard_size,
                "shard_count": 0,
                "total": 0,
                "next_id": 1,
                "created_at": time.time(),
            })
        return read_json(self.meta_path, {})

    def _read_meta(self) -> dict:
        return read_json(self.meta_path, {
            "name": self.name,
            "shard_size": self.shard_size,
            "shard_count": 0,
            "total": 0,
            "next_id": 1,
        })

    def _write_meta(self, meta: dict) -> None:
        atomic_write_json(self.meta_path, meta)

    def _shard_path(self, index: int) -> str:
        return os.path.join(self.dir, f"shard_{index:06d}.json")

    # -- 读写原语 ---------------------------------------------------------
    def _read_shard(self, index: int) -> list:
        path = self._shard_path(index)
        data = read_json(path, [])
        return data if isinstance(data, list) else []

    def _write_shard(self, index: int, records: list) -> None:
        atomic_write_json(self._shard_path(index), records)

    # -- 插入 -------------------------------------------------------------
    def insert(self, record: dict) -> str:
        """插入一条记录，返回其 id（排他锁保护整个读-改-写）。"""
        if not isinstance(record, dict):
            raise TypeError("record 必须是 dict")
        record = dict(record)
        with FileLock(lock_path_for(self.meta_path)):
            meta = self._read_meta()
            record_id = record.get("id") or f"{self.name}_{meta['next_id']}"
            meta["next_id"] += 1
            record["id"] = record_id
            record.setdefault("created_at", time.time())

            index = meta["shard_count"] - 1 if meta["shard_count"] else -1
            if index < 0:
                index = 0
                meta["shard_count"] = 1
                self._write_shard(index, [record])
            else:
                records = self._read_shard(index)
                if len(records) >= self.shard_size:
                    index += 1
                    meta["shard_count"] = index + 1
                    records = [record]
                else:
                    records = records + [record]
                self._write_shard(index, records)

            meta["total"] += 1
            self._write_meta(meta)
            return record_id

    def insert_many(self, records: Iterable[dict]) -> list[str]:
        """批量插入，整体一次性完成（要么全部成功，要么抛出）。"""
        records = list(records)
        ids: list[str] = []
        if not records:
            return ids
        with FileLock(lock_path_for(self.meta_path)):
            meta = self._read_meta()
            pending = []
            for record in records:
                record = dict(record)
                rid = record.get("id") or f"{self.name}_{meta['next_id']}"
                meta["next_id"] += 1
                record["id"] = rid
                record.setdefault("created_at", time.time())
                pending.append(record)
                ids.append(rid)

            index = meta["shard_count"] - 1 if meta["shard_count"] else -1
            if index < 0:
                index = 0
                meta["shard_count"] = 1
            while pending:
                records = self._read_shard(index)
                room = self.shard_size - len(records)
                if room <= 0:
                    index += 1
                    meta["shard_count"] = max(meta["shard_count"], index + 1)
                    continue
                take = pending[:room]
                self._write_shard(index, records + take)
                pending = pending[room:]
                if pending:
                    index += 1
                    meta["shard_count"] = max(meta["shard_count"], index + 1)
            meta["total"] += len(ids)
            self._write_meta(meta)
        return ids

    # -- 读取 -------------------------------------------------------------
    def _iter_all_locked(self) -> Iterator[dict]:
        meta = self._read_meta()
        for index in range(meta.get("shard_count", 0)):
            for record in self._read_shard(index):
                if not record.get("_deleted"):
                    yield record

    def all(self) -> list[dict]:
        with FileLock(lock_path_for(self.meta_path), mode="shared"):
            return list(self._iter_all_locked())

    def get(self, record_id: str) -> Optional[dict]:
        with FileLock(lock_path_for(self.meta_path), mode="shared"):
            for record in self._iter_all_locked():
                if record.get("id") == record_id:
                    return record
        return None

    def get_many(self, record_ids: Iterable[str]) -> list[dict]:
        wanted = set(record_ids)
        with FileLock(lock_path_for(self.meta_path), mode="shared"):
            return [r for r in self._iter_all_locked() if r.get("id") in wanted]

    def query(self, where: Optional[list] = None,
              order_by: Optional[str] = None,
              order: str = "asc",
              limit: Optional[int] = None,
              offset: int = 0) -> list[dict]:
        """跨分片查询：过滤 + 排序 + 分页，返回不重不漏的连续结果。"""
        conditions = where or []
        matched = []
        with FileLock(lock_path_for(self.meta_path), mode="shared"):
            for record in self._iter_all_locked():
                if self._matches(record, conditions):
                    matched.append(record)

        if order_by is not None:
            matched.sort(key=lambda r: _sortable(_extract(r, order_by)),
                         reverse=(order == "desc"))
        if offset:
            matched = matched[offset:]
        if limit is not None:
            matched = matched[:limit]
        return matched

    @staticmethod
    def _matches(record: dict, conditions: list) -> bool:
        for field, op, value in conditions:
            actual = record
            for part in field.split("."):
                if not isinstance(actual, dict):
                    actual = None
                    break
                actual = actual.get(part)
            func = _OPERATORS.get(op)
            if func is None:
                raise ValueError(f"未知操作符: {op}")
            try:
                if not func(actual, value):
                    return False
            except (TypeError, ValueError):
                return False
        return True

    # -- 更新 / 删除 ------------------------------------------------------
    def update(self, record_id: str, patch: dict) -> Optional[dict]:
        """合并式更新：读-改-写都在排他锁内完成，返回更新后的记录。"""
        with FileLock(lock_path_for(self.meta_path)):
            meta = self._read_meta()
            for index in range(meta.get("shard_count", 0)):
                records = self._read_shard(index)
                merged = None
                for i, record in enumerate(records):
                    if record.get("id") == record_id and not record.get("_deleted"):
                        merged = dict(record)
                        merged.update(patch)
                        merged["updated_at"] = time.time()
                        records[i] = merged
                        break
                if merged is not None:
                    self._write_shard(index, records)
                    return merged
        return None

    def delete(self, record_id: str) -> bool:
        """逻辑删除（墓碑标记），压缩时清理。"""
        with FileLock(lock_path_for(self.meta_path)):
            meta = self._read_meta()
            for index in range(meta.get("shard_count", 0)):
                records = self._read_shard(index)
                for i, record in enumerate(records):
                    if record.get("id") == record_id and not record.get("_deleted"):
                        records[i] = {"id": record_id, "_deleted": True}
                        self._write_shard(index, records)
                        meta["total"] = max(0, meta["total"] - 1)
                        self._write_meta(meta)
                        return True
        return False

    # -- 维护 -------------------------------------------------------------
    def compact(self) -> dict:
        with FileLock(lock_path_for(self.meta_path)):
            live = [r for r in self._iter_all_locked()]
            meta = self._read_meta()
            old_count = meta.get("shard_count", 0)
            new_count = (len(live) + self.shard_size - 1) // self.shard_size

            for index in range(old_count):
                path = self._shard_path(index)
                if index < new_count:
                    start = index * self.shard_size
                    self._write_shard(index, live[start:start + self.shard_size])
                else:
                    try:
                        os.remove(path)
                    except FileNotFoundError:
                        pass
            meta["shard_count"] = new_count
            meta["total"] = len(live)
            self._write_meta(meta)
            return {"name": self.name, "before_shards": old_count,
                    "after_shards": new_count, "records": len(live)}

    def stats(self) -> dict:
        meta = self._read_meta()
        return {"name": self.name, "shard_count": meta.get("shard_count", 0),
                "total": meta.get("total", 0), "shard_size": meta.get("shard_size", self.shard_size)}


class StoreRegistry:
    """按实体名管理多个 :class:`ShardedStore` 的注册表，进程内缓存实例。"""

    def __init__(self, root: str, shard_size: int = DEFAULT_SHARD_SIZE):
        self.root = root
        self.shard_size = shard_size
        self._stores: dict[str, ShardedStore] = {}
        self._lock = threading.Lock()

    def store(self, name: str) -> ShardedStore:
        with self._lock:
            if name not in self._stores:
                self._stores[name] = ShardedStore(self.root, name, self.shard_size)
            return self._stores[name]

    def names(self) -> list[str]:
        if not os.path.isdir(self.root):
            return []
        result = []
        for d in os.listdir(self.root):
            full = os.path.join(self.root, d)
            if os.path.isdir(full) and os.path.exists(os.path.join(full, "meta.json")):
                result.append(d)
        return sorted(result)
