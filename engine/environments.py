"""环境管理：配置、依赖解析、工作区隔离。

平台支持一个项目维护多套测试环境（如 dev / staging / prod），每套环境有：

- ``variables``     环境变量（执行时注入用例变量表）
- ``dependencies``  依赖清单（名称 + 版本约束），可一键「解析 / 刷新」
- ``config``        运行参数（base_url、latency_ms、fail_rate 等），
                    直接决定 :class:`~engine.executor.MockTarget` 的行为
- 独立工作区目录 ``data/envs/<env_id>/workspace``，环境之间互不污染

「环境隔离」的落地方式
----------------------
1. 每个环境有独立的磁盘工作区，临时产物、依赖快照分开存放；
2. 执行时把该环境的 ``variables`` + ``config`` 作为一份**快照**传给执行器，
   同一用例在不同环境跑，变量与行为参数完全不同；
3. 依赖解析是纯函数式的「快照解析」，不修改全局状态，并发刷新也安全。

依赖解析为模拟实现：内置一个常见包的最新版本表，按语义化版本约束做简单
匹配（支持 ``>=`` / ``==`` / ``<`` / 无约束取最新）。
"""

from __future__ import annotations

import os
import re
from typing import Any, Optional

from .models import new_id


# 内置「包仓库」：名称 -> 可用版本列表（从旧到新）
_PACKAGE_VERSIONS: dict[str, list[str]] = {
    "requests": ["2.20.0", "2.25.1", "2.28.2", "2.31.0"],
    "pytest": ["6.2.5", "7.4.0", "8.1.1"],
    "flask": ["2.2.5", "3.0.3"],
    "numpy": ["1.21.6", "1.24.4", "2.0.0"],
    "django": ["4.2.11", "5.0.6"],
    "selenium": ["4.9.1", "4.18.1"],
    "jinja2": ["3.1.4"],
    "httpx": ["0.24.1", "0.27.0"],
}


def _version_tuple(v: str) -> tuple:
    return tuple(int(x) for x in re.findall(r"\d+", v)[:3])


def _satisfies(version: str, constraint: str) -> bool:
    """判断版本是否满足 ``>=x`` / ``==x`` / ``<x`` / ``>x`` 这类简单约束。"""
    constraint = constraint.strip()
    if not constraint or constraint == "*":
        return True
    m = re.match(r"^(>=|<=|==|>|<)\s*(.+)$", constraint)
    if not m:
        # 视为精确版本
        return version == constraint
    op, target = m.group(1), m.group(2)
    a, b = _version_tuple(version), _version_tuple(target)
    if op == ">=":
        return a >= b
    if op == "<=":
        return a <= b
    if op == "==":
        return a == b
    if op == ">":
        return a > b
    if op == "<":
        return a < b
    return False


def _latest(versions: list[str]) -> str:
    return sorted(versions, key=_version_tuple)[-1]


def resolve_dependency(name: str, constraint: str) -> Optional[dict]:
    """解析单个依赖，返回 {name, constraint, resolved, latest, status}。"""
    versions = _PACKAGE_VERSIONS.get(name)
    if versions is None:
        return {"name": name, "constraint": constraint, "resolved": None,
                "latest": None, "status": "unknown",
                "message": "未知包（模拟仓库中不存在）"}
    latest = _latest(versions)
    candidates = [v for v in versions if _satisfies(v, constraint)]
    if not candidates:
        return {"name": name, "constraint": constraint, "resolved": None,
                "latest": latest, "status": "conflict",
                "message": f"没有满足 {constraint} 的版本"}
    return {"name": name, "constraint": constraint,
            "resolved": _latest(candidates), "latest": latest,
            "status": "resolved",
            "message": f"解析到 {_latest(candidates)}"}


class EnvironmentManager:
    """环境管理：包裹环境实体存储，提供依赖解析与工作区隔离。"""

    def __init__(self, registry, data_root: str):
        self.registry = registry
        self.data_root = data_root
        self._store = registry.store("environments")

    # -- CRUD -------------------------------------------------------------
    def create(self, project_id: str, payload: dict) -> dict:
        env = {
            "id": new_id("env"),
            "project_id": project_id,
            "name": payload.get("name", "未命名环境"),
            "description": payload.get("description", ""),
            "python_version": payload.get("python_version", "3.11"),
            "base_image": payload.get("base_image", "python:3.11-slim"),
            "variables": payload.get("variables") or {},
            "dependencies": payload.get("dependencies") or [],
            "config": payload.get("config") or {
                "base_url": "http://mock.local",
                "latency_ms": 20,
                "fail_rate": 0.0,
            },
        }
        self._store.insert(env)
        self.ensure_workspace(env["id"])
        return env

    def list(self, project_id: str) -> list[dict]:
        return self._store.query(where=[("project_id", "eq", project_id)],
                                 order_by="created_at", order="asc")

    def get(self, env_id: str) -> Optional[dict]:
        return self._store.get(env_id)

    def update(self, env_id: str, patch: dict) -> Optional[dict]:
        return self._store.update(env_id, patch)

    def delete(self, env_id: str) -> bool:
        return self._store.delete(env_id)

    # -- 依赖解析 ---------------------------------------------------------
    def resolve(self, env_id: str) -> dict:
        env = self.get(env_id)
        if env is None:
            return {"error": "环境不存在"}
        resolved = [resolve_dependency(d["name"], d.get("constraint", "*"))
                    for d in env.get("dependencies", [])]
        ok_count = sum(1 for r in resolved if r["status"] == "resolved")
        conflict_count = sum(1 for r in resolved if r["status"] == "conflict")
        return {
            "env_id": env_id,
            "dependencies": resolved,
            "ok": conflict_count == 0,
            "resolved_count": ok_count,
            "conflict_count": conflict_count,
        }

    # -- 工作区隔离 -------------------------------------------------------
    def workspace_dir(self, env_id: str) -> str:
        return os.path.join(self.data_root, "envs", env_id, "workspace")

    def ensure_workspace(self, env_id: str) -> str:
        d = self.workspace_dir(env_id)
        os.makedirs(d, exist_ok=True)
        return d

    # -- 执行快照 ---------------------------------------------------------
    def snapshot(self, env_id: str) -> dict:
        """取环境执行快照：变量 + 运行配置（供执行器使用）。"""
        env = self.get(env_id)
        if env is None:
            return {"variables": {}, "config": {}}
        return {
            "variables": dict(env.get("variables") or {}),
            "config": dict(env.get("config") or {}),
        }

    def to_executor_config(self, env_id: str) -> dict:
        snap = self.snapshot(env_id)
        cfg = dict(snap["config"])
        cfg["variables"] = snap["variables"]
        return cfg
