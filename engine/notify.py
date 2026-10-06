"""通知与集成。

支持 webhook / slack / email / dingtalk 四类集成。平台离线运行，投递为
**模拟投递**：不发起真实网络请求，而是按集成类型 + 事件确定性地给出
「已送达 / 失败」结果与延迟，并把每次投递记入事件日志，供「通知与集成」
页面查看历史、验证配置。

事件类型：
- ``build.finished``  构建结束（成功或失败都发，若订阅）
- ``build.passed``    构建成功
- ``build.failed``    构建失败（含 error / cancelled）
- ``test``            测试投递

集成只投递它订阅的事件（``events`` 字段）。
"""

from __future__ import annotations

import hashlib
import time
from typing import Optional

from .models import INTEGRATION_TYPES, new_id

# 各类型的默认目标，仅用于展示（不真实发送）
_TYPE_LABEL = {
    "webhook": "Webhook",
    "slack": "Slack",
    "email": "Email",
    "dingtalk": "钉钉",
}


def _seeded_int(*parts) -> int:
    h = hashlib.md5("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    return int(h[:8], 16)


class NotificationManager:
    """通知与集成管理。"""

    def __init__(self, registry):
        self._store = registry.store("integrations")
        self._events = registry.store("notify_events")

    # -- 集成 CRUD --------------------------------------------------------
    def create(self, project_id: str, payload: dict) -> dict:
        itype = payload.get("type", "webhook")
        if itype not in INTEGRATION_TYPES:
            itype = "webhook"
        integration = {
            "id": new_id("int"),
            "project_id": project_id,
            "type": itype,
            "name": payload.get("name", _TYPE_LABEL[itype]),
            "enabled": bool(payload.get("enabled", True)),
            "config": payload.get("config") or {},
            "events": payload.get("events") or ["build.finished"],
        }
        self._store.insert(integration)
        return integration

    def list(self, project_id: str) -> list[dict]:
        return self._store.query(where=[("project_id", "eq", project_id)],
                                 order_by="created_at", order="asc")

    def get(self, integration_id: str) -> Optional[dict]:
        return self._store.get(integration_id)

    def update(self, integration_id: str, patch: dict) -> Optional[dict]:
        return self._store.update(integration_id, patch)

    def delete(self, integration_id: str) -> bool:
        return self._store.delete(integration_id)

    # -- 投递（模拟） -----------------------------------------------------
    def _deliver(self, integration: dict, event: str, payload: dict) -> dict:
        itype = integration.get("type", "webhook")
        target = (integration.get("config") or {}).get("url") or \
            (integration.get("config") or {}).get("address") or \
            (integration.get("config") or {}).get("channel") or "未配置目标"

        # 确定性投递结果：未配置目标 / 显式 fail 标志 → 失败
        latency = 8 + _seeded_int(integration["id"], event, payload.get("build_id")) % 120
        failed = not target or target == "未配置目标" or \
            (integration.get("config") or {}).get("fail", False)
        return {
            "status": "failed" if failed else "delivered",
            "recipient": target,
            "latency_ms": latency,
        }

    def _record(self, integration: dict, event: str, payload: dict,
                outcome: dict) -> dict:
        record = {
            "id": new_id("evt"),
            "project_id": integration.get("project_id"),
            "integration_id": integration["id"],
            "integration_name": integration.get("name"),
            "type": integration.get("type"),
            "event": event,
            "status": outcome["status"],
            "recipient": outcome["recipient"],
            "latency_ms": outcome["latency_ms"],
            "payload": payload,
            "created_at": time.time(),
        }
        self._events.insert(record)
        return record

    def fire(self, project_id: str, event: str, payload: dict) -> list[dict]:
        """向订阅了该事件的所有启用集成投递通知，返回事件记录列表。"""
        delivered: list[dict] = []
        for integration in self.list(project_id):
            if not integration.get("enabled", True):
                continue
            if event not in (integration.get("events") or ["build.finished"]):
                continue
            outcome = self._deliver(integration, event, payload)
            delivered.append(self._record(integration, event, payload, outcome))
        return delivered

    def send_test(self, integration_id: str) -> dict:
        integration = self.get(integration_id)
        if integration is None:
            return {"error": "集成不存在"}
        outcome = self._deliver(integration, "test", {"test": True})
        record = self._record(integration, "test", {"test": True}, outcome)
        record["integration"] = integration
        return record

    def events(self, project_id: str, limit: int = 100) -> list[dict]:
        return self._events.query(where=[("project_id", "eq", project_id)],
                                  order_by="created_at", order="desc",
                                  limit=limit)
