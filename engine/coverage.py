"""代码覆盖率分析（模拟）。

平台没有真实仓库，覆盖率由「模拟源码树 + 确定性伪随机」生成：每个项目
有一组固定的模拟模块（文件 → 行数），每次构建按 ``build_id`` 作随机种子
计算每个文件覆盖的行数与百分比。这样：

- 同一构建多次读取覆盖率**稳定一致**（种子确定）；
- 不同构建之间覆盖率有波动，可用于画出趋势；
- 覆盖率与构建通过率弱相关（通过率越高，平均覆盖率略高），
  体现「测试跑得越充分，覆盖越全」的直觉。
"""

from __future__ import annotations

import hashlib
import random
from typing import Any

# 模拟源码树：模块路径 -> 行数（每个项目一致，保证跨项目口径可比）
_MODULES: list[tuple[str, int]] = [
    ("src/api/auth.py", 420),
    ("src/api/users.py", 368),
    ("src/api/projects.py", 290),
    ("src/core/executor.py", 512),
    ("src/core/scheduler.py", 448),
    ("src/core/validator.py", 236),
    ("src/core/report.py", 305),
    ("src/storage/lock.py", 188),
    ("src/storage/sharded.py", 402),
    ("src/utils/retry.py", 122),
    ("src/utils/log.py", 96),
    ("src/utils/http.py", 158),
]


def _seed(*parts: Any) -> int:
    h = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    return int(h[:12], 16)


class CoverageAnalyzer:
    """覆盖率分析器。"""

    def __init__(self, build_store_registry):
        self.builds = build_store_registry

    # -- 单次构建 ---------------------------------------------------------
    def generate(self, project_id: str, build_id: str,
                 passed_ratio: float = 1.0) -> dict:
        """生成并缓存一次构建的覆盖率报告。"""
        rng = random.Random(_seed(project_id, build_id, "coverage"))
        files = []
        total_lines = 0
        total_covered = 0
        for path, lines in _MODULES:
            # 通过率越高，覆盖率基线越高；叠加每文件独立的确定性波动
            base = 0.45 + 0.40 * max(0.0, min(1.0, passed_ratio))
            jitter = rng.uniform(-0.18, 0.18)
            ratio = max(0.05, min(0.99, base + jitter))
            covered = int(round(lines * ratio))
            total_lines += lines
            total_covered += covered
            files.append({
                "file": path,
                "lines": lines,
                "covered": covered,
                "missed": lines - covered,
                "percent": round(covered / lines * 100, 1),
            })
        percent = round(total_covered / total_lines * 100, 1)
        coverage = {
            "project_id": project_id,
            "build_id": build_id,
            "percent": percent,
            "total_lines": total_lines,
            "covered_lines": total_covered,
            "missed_lines": total_lines - total_covered,
            "files": sorted(files, key=lambda f: f["percent"]),
            "generated_at": __import__("time").time(),
        }
        self.builds.for_project(project_id).write_coverage(build_id, coverage)
        return coverage

    def get(self, project_id: str, build_id: str) -> dict:
        cov = self.builds.for_project(project_id).read_coverage(build_id)
        if cov is None:
            build = self.builds.for_project(project_id).get(build_id)
            passed_ratio = 1.0
            if build:
                total = build.get("total", 0)
                passed = build.get("passed", 0)
                passed_ratio = (passed / total) if total else 1.0
            return self.generate(project_id, build_id, passed_ratio)
        return cov

    # -- 趋势 -------------------------------------------------------------
    def trend(self, project_id: str, limit: int = 20) -> dict:
        store = self.builds.for_project(project_id)
        builds = store.list_builds()[:limit]
        points = []
        for b in reversed(builds):  # 时间正序
            cov = self.get(project_id, b["id"])
            points.append({
                "build_id": b["id"],
                "name": b.get("name") or b["id"],
                "status": b.get("status"),
                "percent": cov["percent"],
                "passed_ratio": (b.get("passed", 0) / b["total"]) if b.get("total") else 0,
                "finished_at": b.get("finished_at"),
            })
        return {"project_id": project_id, "points": points}
