"""调度器集成测试。

覆盖：并发调度（构建池 + 用例池）、结果收集与聚合、报告/覆盖率/通知收尾、
取消、以及定时任务的触发去重。
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import (CoverageAnalyzer, DefectManager, EnvironmentManager,
                    NotificationManager, ReportGenerator, Scheduler, TestExecutor)
from storage import BuildStoreRegistry, StoreRegistry


def _make_scheduler(data_root):
    registry = StoreRegistry(os.path.join(data_root, "store"), shard_size=50)
    builds = BuildStoreRegistry(os.path.join(data_root, "builds"))
    executor = TestExecutor()
    env_mgr = EnvironmentManager(registry, data_root)
    coverage = CoverageAnalyzer(builds)
    report = ReportGenerator(builds)
    defects = DefectManager(registry)
    notify = NotificationManager(registry)
    sched = Scheduler(registry, builds, executor, env_mgr, report, coverage,
                      defects, notify, max_build_workers=2, max_case_workers=4,
                      tick_seconds=0.2)
    return registry, builds, env_mgr, sched


class TestSchedulerEndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry, self.builds, self.env_mgr, self.sched = _make_scheduler(self.tmp.name)

    def tearDown(self):
        self.sched.shutdown()
        self.tmp.cleanup()

    def _setup_project(self, n_cases=12):
        pid = self.registry.store("projects").insert({"name": "P"})
        env = self.env_mgr.create(pid, {"name": "dev", "config": {"latency_ms": 0, "fail_rate": 0.0}})
        cases_store = self.registry.store("cases")
        ids = []
        for i in range(n_cases):
            ids.append(cases_store.insert({
                "id": f"case_{i}", "project_id": pid, "name": f"用例{i}",
                "priority": "P2", "tags": ["g1" if i % 2 else "g2"], "timeout": 30,
                "steps": [
                    {"action": "request", "method": "GET", "url": "/api/health"},
                    {"action": "assert", "type": "status", "actual": "${resp.status}", "expected": 200},
                ],
            }))
        suite = {
            "id": "suite_1", "project_id": pid, "name": "冒烟",
            "env_id": env["id"], "case_ids": ids,
        }
        self.registry.store("suites").insert(suite)
        return pid, suite

    def test_full_run_collects_results(self):
        pid, suite = self._setup_project(12)
        result = self.sched.submit_build(pid, suite["id"], trigger="manual")
        self.assertIn("id", result)
        build_id = result["id"]

        # 等待构建完成
        deadline = time.time() + 20
        build = None
        while time.time() < deadline:
            build = self.builds.for_project(pid).get(build_id)
            if build and build["status"] in ("passed", "failed", "cancelled", "error"):
                break
            time.sleep(0.05)
        self.assertIsNotNone(build)
        self.assertEqual(build["status"], "passed")
        self.assertEqual(build["passed"], 12)
        self.assertEqual(len(self.builds.for_project(pid).results(build_id)), 12)

        # 报告 / 覆盖率已生成
        self.assertIsNotNone(self.builds.for_project(pid).read_report(build_id))
        self.assertIsNotNone(self.builds.for_project(pid).read_coverage(build_id))

    def test_concurrent_builds(self):
        pid, suite = self._setup_project(20)
        results = [self.sched.submit_build(pid, suite["id"]) for _ in range(3)]
        ids = [r["id"] for r in results]
        deadline = time.time() + 30
        while time.time() < deadline:
            builds = [self.builds.for_project(pid).get(b) for b in ids]
            if all(b and b["status"] in ("passed", "failed", "cancelled", "error") for b in builds):
                break
            time.sleep(0.05)
        for b in self.builds.for_project(pid).list_builds():
            if b["id"] in ids:
                self.assertEqual(b["status"], "passed")
                self.assertEqual(b["passed"], 20)
                self.assertEqual(len(self.builds.for_project(pid).results(b["id"])), 20)

    def test_cancel_build(self):
        pid, suite = self._setup_project(30)
        result = self.sched.submit_build(pid, suite["id"])
        build_id = result["id"]
        self.sched.cancel_build(build_id)
        deadline = time.time() + 15
        while time.time() < deadline:
            b = self.builds.for_project(pid).get(build_id)
            if b and b["status"] in ("cancelled", "passed", "failed"):
                break
            time.sleep(0.05)
        b = self.builds.for_project(pid).get(build_id)
        self.assertIn(b["status"], ("cancelled", "passed", "failed"))

    def test_schedule_fires_once_per_minute(self):
        pid, suite = self._setup_project(3)
        cron = "* * * * *"  # 每分钟
        sch = {
            "id": "sch_1", "project_id": pid, "name": "每分", "cron": cron,
            "suite_id": suite["id"], "env_id": suite["env_id"], "enabled": True,
            "last_fired_minute": None,
        }
        self.registry.store("schedules").insert(sch)
        self.sched.start()
        # 直接调用一次扫描，再立即调用一次，同分钟只应触发一次
        self.sched._scan_schedules()
        self.sched._scan_schedules()
        runs = self.registry.store("schedule_runs").query(where=[("schedule_id", "eq", sch["id"])])
        self.assertEqual(len(runs), 1)


if __name__ == "__main__":
    unittest.main()
