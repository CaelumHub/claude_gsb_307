"""按「项目 + 构建」二次分片的构建结果存储。

这是平台最核心、也最容易出错的存储：一次构建会并发跑几十上百个用例，
每个用例结束的瞬间都要把结果写回磁盘，同时要维护「通过 / 失败 / 超时」
计数与分组聚合、追加实时日志。多个 worker 线程同时写，如果各自
「读-改-写」没有临界区，就会出现丢失更新、计数与结果条数对不上、
日志缺行等问题。

设计
----
文件布局（``data/builds/<project_id>/<build_id>/``）::

    build.json            构建元数据 + 状态 + 聚合计数（唯一事实源）
    results/
      meta.json           结果分片元数据（分片数 / 总数）
      shard_000001.json   用例结果数组（大小受控，可跨分片查询）
    logs/
      meta.json           日志序号计数器 {next_seq}
      build.log           追加式实时日志（行号即序号）
      <case_id>.log       单个用例日志（报告详情用）
    coverage.json         覆盖率快照（生成后缓存）
    report.json           报告缓存（结果变化后失效重算）
    .build.lock           本构建的写锁文件

一致性保证
----------
- 一个构建一个锁文件 ``.build.lock``：**所有** 对该构建的写
  （计数更新、结果落盘、日志追加、状态迁移）都先拿这把排他锁，
  因此计数、结果条数、日志序号三者永远一致；
- ``build.json`` 是唯一事实源，计数与聚合都写在这里，避免多文件之间
  因崩溃产生的分叉；
- 写文件走原子替换，日志追加在锁内进行（``open(a)`` 追加不会覆盖）。

这样，「结果收集与聚合」在并发下天然安全：锁把并发写串行化成一个有序
事件流，谁先拿到锁谁先写，绝不丢失。
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any, Iterable, Optional

from .atomic import atomic_write_json, read_json
from .lock import FileLock, lock_path_for


RESULT_SHARD_SIZE = 500

_STATUSES = ("pending", "running", "passed", "failed", "cancelled", "error")


def _empty_build(build_id: str, project_id: str, **kw: Any) -> dict:
    build = {
        "id": build_id,
        "project_id": project_id,
        "suite_id": kw.get("suite_id"),
        "env_id": kw.get("env_id"),
        "name": kw.get("name", ""),
        "trigger": kw.get("trigger", "manual"),
        "status": "pending",
        "total": 0,
        "passed": 0,
        "failed": 0,
        "error": 0,
        "skipped": 0,
        "timeout": 0,
        "started_at": None,
        "finished_at": None,
        "duration": 0.0,
        "by_group": {},
        "by_priority": {},
        "durations": [],
        "created_at": time.time(),
    }
    return build


class BuildStore:
    """单个项目下的构建结果存储。"""

    def __init__(self, builds_root: str, project_id: str):
        self.builds_root = builds_root
        self.project_id = project_id
        self.dir = os.path.join(builds_root, project_id)
        os.makedirs(self.dir, exist_ok=True)

    # -- 路径 -------------------------------------------------------------
    def _build_dir(self, build_id: str) -> str:
        return os.path.join(self.dir, build_id)

    def _build_path(self, build_id: str) -> str:
        return os.path.join(self._build_dir(build_id), "build.json")

    def _lock(self, build_id: str) -> str:
        return os.path.join(self._build_dir(build_id), ".build.lock")

    def _results_meta_path(self, build_id: str) -> str:
        return os.path.join(self._build_dir(build_id), "results", "meta.json")

    def _shard_path(self, build_id: str, index: int) -> str:
        return os.path.join(self._build_dir(build_id), "results", f"shard_{index:06d}.json")

    def _logs_dir(self, build_id: str) -> str:
        return os.path.join(self._build_dir(build_id), "logs")

    def _log_meta_path(self, build_id: str) -> str:
        return os.path.join(self._logs_dir(build_id), "meta.json")

    def _build_log_path(self, build_id: str) -> str:
        return os.path.join(self._logs_dir(build_id), "build.log")

    def _case_log_path(self, build_id: str, case_id: str) -> str:
        return os.path.join(self._logs_dir(build_id), f"{case_id}.log")

    # -- 构建生命周期 -----------------------------------------------------
    def create(self, build_id: str, **kw: Any) -> dict:
        build = _empty_build(build_id, self.project_id, **kw)
        bdir = self._build_dir(build_id)
        os.makedirs(os.path.join(bdir, "results"), exist_ok=True)
        os.makedirs(os.path.join(bdir, "logs"), exist_ok=True)
        atomic_write_json(self._results_meta_path(build_id),
                          {"shard_count": 0, "total": 0})
        atomic_write_json(self._log_meta_path(build_id), {"next_seq": 0})
        atomic_write_json(self._build_path(build_id), build)
        return build

    def exists(self, build_id: str) -> bool:
        return os.path.exists(self._build_path(build_id))

    def get(self, build_id: str) -> Optional[dict]:
        if not self.exists(build_id):
            return None
        with FileLock(self._lock(build_id), mode="shared"):
            return read_json(self._build_path(build_id), None)

    def update(self, build_id: str, patch: dict) -> dict:
        """原子地合并更新构建字段，返回更新后的构建。"""
        with FileLock(self._lock(build_id)):
            build = read_json(self._build_path(build_id), _empty_build(build_id, self.project_id))
            build.update(patch)
            atomic_write_json(self._build_path(build_id), build)
            return build

    def set_total(self, build_id: str, total: int) -> dict:
        return self.update(build_id, {"total": total, "status": "running",
                                      "started_at": time.time()})

    def finish(self, build_id: str, status: str) -> dict:
        """结束构建：写入终态、结束时间与总耗时。"""
        with FileLock(self._lock(build_id)):
            build = read_json(self._build_path(build_id), _empty_build(build_id, self.project_id))
            build["status"] = status
            build["finished_at"] = time.time()
            if build.get("started_at"):
                build["duration"] = round(build["finished_at"] - build["started_at"], 3)
            atomic_write_json(self._build_path(build_id), build)
            return build

    def list_builds(self) -> list[dict]:
        """列出本项目所有构建，按创建时间倒序。"""
        if not os.path.isdir(self.dir):
            return []
        builds = []
        for bid in os.listdir(self.dir):
            bpath = self._build_path(bid)
            if os.path.isfile(bpath):
                builds.append(read_json(bpath, None))
        builds = [b for b in builds if b]
        builds.sort(key=lambda b: b.get("created_at", 0), reverse=True)
        return builds

    def delete(self, build_id: str) -> bool:
        import shutil
        bdir = self._build_dir(build_id)
        if os.path.isdir(bdir):
            shutil.rmtree(bdir, ignore_errors=True)
            return True
        return False

    # -- 结果写入（并发安全的收集与聚合） --------------------------------
    def record_result(self, build_id: str, result: dict) -> dict:
        """写入一个用例结果，并同步更新聚合计数。

        整个「更新计数 → 落结果分片 → 追加日志」都在本构建的排他锁内，
        保证并发下计数、结果、日志三者严格一致。返回写后的构建摘要。
        """
        result = dict(result)
        result.setdefault("finished_at", time.time())
        with FileLock(self._lock(build_id)):
            build = read_json(self._build_path(build_id), _empty_build(build_id, self.project_id))

            # 1) 更新聚合计数
            status = result.get("status", "error")
            if status in ("passed", "failed", "error", "skipped", "timeout"):
                build[status] = build.get(status, 0) + 1
            build["durations"] = build.get("durations", []) + [result.get("duration", 0.0)]

            group = result.get("group") or "默认"
            gp = build.setdefault("by_group", {})
            entry = gp.setdefault(group, {"total": 0, "passed": 0, "failed": 0,
                                          "error": 0, "skipped": 0, "timeout": 0,
                                          "duration": 0.0})
            entry["total"] += 1
            entry[status if status in entry else "error"] += 1
            entry["duration"] = round(entry["duration"] + result.get("duration", 0.0), 3)

            priority = result.get("priority") or "P3"
            pp = build.setdefault("by_priority", {})
            pe = pp.setdefault(priority, {"total": 0, "passed": 0, "failed": 0,
                                          "error": 0, "skipped": 0, "timeout": 0})
            pe["total"] += 1
            pe[status if status in pe else "error"] += 1

            # 2) 结果落分片
            meta = read_json(self._results_meta_path(build_id), {"shard_count": 0, "total": 0})
            index = meta["shard_count"] - 1 if meta["shard_count"] else -1
            if index < 0:
                index = 0
                meta["shard_count"] = 1
                self._write_shard(build_id, index, [result])
            else:
                records = self._read_shard(build_id, index)
                if len(records) >= RESULT_SHARD_SIZE:
                    index += 1
                    meta["shard_count"] = index + 1
                    records = [result]
                else:
                    records = records + [result]
                self._write_shard(build_id, index, records)
            meta["total"] += 1
            atomic_write_json(self._results_meta_path(build_id), meta)

            # 3) 追加日志
            for line in result.get("logs", []):
                self._append_log_locked(build_id, line)
            self._append_log_locked(
                build_id,
                f"[{result.get('status', 'error').upper()}] {result.get('case_name', result.get('case_id', ''))}"
                f" ({round(result.get('duration', 0.0), 3)}s)",
            )

            # 4) 写回唯一事实源
            atomic_write_json(self._build_path(build_id), build)
            return build

    def _write_shard(self, build_id: str, index: int, records: list) -> None:
        atomic_write_json(self._shard_path(build_id, index), records)

    def _read_shard(self, build_id: str, index: int) -> list:
        data = read_json(self._shard_path(build_id, index), [])
        return data if isinstance(data, list) else []

    # -- 日志 -------------------------------------------------------------
    def _append_log_locked(self, build_id: str, line: str) -> int:
        """在持有锁的前提下追加一行日志，返回新序号。"""
        log_meta = read_json(self._log_meta_path(build_id), {"next_seq": 0})
        with open(self._build_log_path(build_id), "a", encoding="utf-8") as fh:
            fh.write(str(line) + "\n")
        log_meta["next_seq"] += 1
        atomic_write_json(self._log_meta_path(build_id), log_meta)
        return log_meta["next_seq"]

    def append_log(self, build_id: str, line: str) -> int:
        with FileLock(self._lock(build_id)):
            return self._append_log_locked(build_id, line)

    def read_logs(self, build_id: str, after: int = 0, limit: int = 2000) -> dict:
        """读取 ``after`` 序号之后的日志行（用于实时监控轮询）。"""
        with FileLock(self._lock(build_id), mode="shared"):
            log_meta = read_json(self._log_meta_path(build_id), {"next_seq": 0})
            next_seq = log_meta.get("next_seq", 0)
            if after >= next_seq:
                return {"lines": [], "next_seq": next_seq, "eof": True}
            try:
                with open(self._build_log_path(build_id), "r", encoding="utf-8") as fh:
                    text = fh.read()
            except OSError:
                return {"lines": [], "next_seq": next_seq, "eof": True}
            lines = text.split("\n")
            # 去掉末尾的空串
            if lines and lines[-1] == "":
                lines = lines[:-1]
            window = lines[after:after + limit]
            return {"lines": window, "next_seq": after + len(window),
                    "eof": after + len(window) >= next_seq}

    def read_case_log(self, build_id: str, case_id: str) -> str:
        with FileLock(self._lock(build_id), mode="shared"):
            try:
                with open(self._case_log_path(build_id, case_id), "r", encoding="utf-8") as fh:
                    return fh.read()
            except OSError:
                return ""

    def write_case_log(self, build_id: str, case_id: str, text: str) -> None:
        with FileLock(self._lock(build_id)):
            path = self._case_log_path(build_id, case_id)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)

    # -- 结果查询 ---------------------------------------------------------
    def results(self, build_id: str, where: Optional[list] = None,
                order_by: Optional[str] = None, order: str = "asc",
                limit: Optional[int] = None, offset: int = 0) -> list[dict]:
        conditions = where or []
        matched = []
        with FileLock(self._lock(build_id), mode="shared"):
            meta = read_json(self._results_meta_path(build_id), {"shard_count": 0, "total": 0})
            for index in range(meta.get("shard_count", 0)):
                for record in self._read_shard(build_id, index):
                    if self._matches(record, conditions):
                        matched.append(record)
        if order_by is not None:
            def _key(r: dict):
                v = r
                for part in order_by.split("."):
                    if not isinstance(v, dict):
                        return ""
                    v = v.get(part)
                return v if v is not None else ""
            matched.sort(key=_key, reverse=(order == "desc"))
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
            if op == "eq":
                ok = actual == value
            elif op == "ne":
                ok = actual != value
            elif op == "in":
                ok = actual in value
            elif op == "contains":
                ok = value in actual if hasattr(actual, "__contains__") else False
            elif op == "gte":
                ok = actual >= value
            elif op == "lte":
                ok = actual <= value
            elif op == "gt":
                ok = actual > value
            elif op == "lt":
                ok = actual < value
            else:
                ok = True
            if not ok:
                return False
        return True

    # -- 覆盖率 / 报告缓存 ------------------------------------------------
    def write_coverage(self, build_id: str, coverage: dict) -> None:
        with FileLock(self._lock(build_id)):
            atomic_write_json(os.path.join(self._build_dir(build_id), "coverage.json"), coverage)

    def read_coverage(self, build_id: str) -> Optional[dict]:
        path = os.path.join(self._build_dir(build_id), "coverage.json")
        return read_json(path, None)

    def write_report(self, build_id: str, report: dict) -> None:
        with FileLock(self._lock(build_id)):
            atomic_write_json(os.path.join(self._build_dir(build_id), "report.json"), report)

    def read_report(self, build_id: str) -> Optional[dict]:
        path = os.path.join(self._build_dir(build_id), "report.json")
        return read_json(path, None)


class BuildStoreRegistry:
    """按项目缓存 :class:`BuildStore` 实例。"""

    def __init__(self, builds_root: str):
        self.builds_root = builds_root
        self._stores: dict[str, BuildStore] = {}
        self._lock = threading.Lock()

    def for_project(self, project_id: str) -> BuildStore:
        with self._lock:
            if project_id not in self._stores:
                self._stores[project_id] = BuildStore(self.builds_root, project_id)
            return self._stores[project_id]

    def all_builds(self) -> list[dict]:
        """跨项目列出全部构建（供全局执行监控）。"""
        root = self.builds_root
        builds: list[dict] = []
        if not os.path.isdir(root):
            return builds
        for pid in os.listdir(root):
            store = self.for_project(pid)
            for b in store.list_builds():
                builds.append(b)
        builds.sort(key=lambda b: b.get("created_at", 0), reverse=True)
        return builds

    def find_build(self, build_id: str) -> Optional[dict]:
        root = self.builds_root
        if not os.path.isdir(root):
            return None
        for pid in os.listdir(root):
            bpath = os.path.join(root, pid, build_id, "build.json")
            if os.path.isfile(bpath):
                return read_json(bpath, None)
        return None
