"""缺陷跟踪。

缺陷与用例 / 构建关联：一次构建里失败的用例，可以一键（或自动）转成缺陷，
缺陷保留来源（``source_case_id`` / ``source_build_id``），方便从报告页跳回
缺陷页闭环处理。

状态流：``open -> in_progress -> fixed -> verified -> closed``，
以及 ``reopened`` 用于重新打开。
"""

from __future__ import annotations

from typing import Optional

from .models import DEFECT_STATUSES, SEVERITIES, new_id


class DefectManager:
    """缺陷管理。"""

    def __init__(self, registry):
        self._store = registry.store("defects")

    def create(self, project_id: str, payload: dict) -> dict:
        severity = payload.get("severity", "major")
        if severity not in SEVERITIES:
            severity = "major"
        defect = {
            "id": new_id("def"),
            "project_id": project_id,
            "title": payload.get("title", "未命名缺陷"),
            "description": payload.get("description", ""),
            "severity": severity,
            "status": payload.get("status", "open"),
            "source_case_id": payload.get("source_case_id"),
            "source_build_id": payload.get("source_build_id"),
            "assignee": payload.get("assignee", ""),
            "tags": payload.get("tags") or [],
        }
        self._store.insert(defect)
        return defect

    def create_from_case(self, project_id: str, case_result: dict,
                         build_id: str) -> Optional[dict]:
        """从失败的用例结果自动生成缺陷。"""
        if case_result.get("status") not in ("failed", "error", "timeout"):
            return None
        reason = ""
        for a in case_result.get("assertions", []):
            if not a.get("ok"):
                reason = a.get("message", "")
                break
        if not reason:
            for s in case_result.get("steps", []):
                if s.get("status") in ("failed", "error"):
                    reason = s.get("message", "")
                    break
        return self.create(project_id, {
            "title": f"[自动] 用例失败: {case_result.get('case_name')}",
            "description": reason or "用例执行失败，请查看构建日志。",
            "severity": "major" if case_result.get("priority") in ("P0", "P1") else "minor",
            "source_case_id": case_result.get("case_id"),
            "source_build_id": build_id,
        })

    def list(self, project_id: str, status: str = None,
             severity: str = None) -> list[dict]:
        where = [("project_id", "eq", project_id)]
        if status:
            where.append(("status", "eq", status))
        if severity:
            where.append(("severity", "eq", severity))
        return self._store.query(where=where, order_by="created_at", order="desc")

    def get(self, defect_id: str) -> Optional[dict]:
        return self._store.get(defect_id)

    def update(self, defect_id: str, patch: dict) -> Optional[dict]:
        status = patch.get("status")
        if status is not None and status not in DEFECT_STATUSES:
            patch["status"] = "open"
        return self._store.update(defect_id, patch)

    def delete(self, defect_id: str) -> bool:
        return self._store.delete(defect_id)

    def stats(self, project_id: str) -> dict:
        defects = self.list(project_id)
        by_status: dict[str, int] = {}
        by_severity: dict[str, int] = {}
        for d in defects:
            by_status[d.get("status", "open")] = by_status.get(d.get("status", "open"), 0) + 1
            by_severity[d.get("severity", "major")] = by_severity.get(d.get("severity", "major"), 0) + 1
        return {"total": len(defects), "by_status": by_status, "by_severity": by_severity}
