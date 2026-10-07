"""套件编排测试。

覆盖：
- 编排计划规范化（依赖环 / 自依赖 / 未知依赖 / 分批 / 批次重排）
- 前置失败阻断全部用例（blocked，与普通 skipped 区分）
- 用例依赖门控：A 失败 -> B blocked（且原因明确），独立用例照常执行
- 依赖阻断向下游传播（A -> B -> C）
- 按优先级分批串行、每批并发受控
- 后置动作总会执行（即使前置失败 / 构建取消）
- 非编排套件仍走旧的平铺执行路径
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import (CoverageAnalyzer, DefectManager, EnvironmentManager,
                    NotificationManager, ReportGenerator, Scheduler, TestExecutor)
from engine.orchestration import PlanError, normalize_plan
from storage import BuildStoreRegistry, StoreRegistry


def _make_scheduler(data_root, max_case_workers=4):
    registry = StoreRegistry(os.path.join(data_root, "store"), shard_size=50)
    builds = BuildStoreRegistry(os.path.join(data_root, "builds"))
    env_mgr = EnvironmentManager(registry, data_root)
    sched = Scheduler(registry, builds, TestExecutor(), env_mgr,
                      ReportGenerator(builds), CoverageAnalyzer(builds),
                      DefectManager(registry), NotificationManager(registry),
                      max_build_workers=2, max_case_workers=max_case_workers,
                      tick_seconds=60)
    return registry, builds, env_mgr, sched


def _case(cid, name, *, fail=False, priority="P2", tags=None, enabled=True):
    steps = [
        {"action": "request", "method": "GET",
         "url": "/api/error" if fail else "/api/health"},
        {"action": "assert", "type": "status", "actual": "${resp.status}",
         "expected": 200},
    ]
    return {"id": cid, "name": name, "priority": priority,
            "tags": tags or ["g"], "timeout": 30, "enabled": enabled,
            "steps": steps}


def _wait(builds, pid, build_id, timeout=20, sched=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        b = builds.for_project(pid).get(build_id)
        if b and b["status"] in ("passed", "failed", "cancelled", "error"):
            break
        time.sleep(0.03)
    else:
        raise AssertionError("构建未在规定时间内结束")
    # 终态在收尾（报告/覆盖率/通知）写入前就会置位；等构建彻底离开运行集合，
    # 避免测试清理临时目录时收尾线程还在写文件。
    if sched is not None:
        while time.time() < deadline and any(r["build_id"] == build_id
                                             for r in sched.running()):
            time.sleep(0.02)
    return b


class TestNormalizePlan(unittest.TestCase):
    def setUp(self):
        self.cases = [_case("a", "A"), _case("b", "B"), _case("c", "C")]

    def test_self_dependency_rejected(self):
        with self.assertRaises(PlanError):
            normalize_plan({"enabled": True,
                            "dependencies": [{"case_id": "a", "depends_on": ["a"]}]},
                           self.cases, 4)

    def test_unknown_dependency_rejected(self):
        with self.assertRaises(PlanError):
            normalize_plan({"enabled": True,
                            "dependencies": [{"case_id": "a", "depends_on": ["x"]}]},
                           self.cases, 4)

    def test_cycle_rejected(self):
        with self.assertRaises(PlanError):
            normalize_plan({"enabled": True, "dependencies": [
                {"case_id": "a", "depends_on": ["b"]},
                {"case_id": "b", "depends_on": ["a"]},
            ]}, self.cases, 4)

    def test_priority_batches(self):
        cases = [_case("a", "A", priority="P0"), _case("b", "B", priority="P2"),
                 _case("c", "C", priority="P0")]
        plan = normalize_plan({"enabled": True,
                               "batching": {"mode": "priority", "concurrency": 2}},
                              cases, 4)
        names = [g["name"] for g in plan["batches"]]
        self.assertEqual(names, ["优先级 P0", "优先级 P2"])
        self.assertEqual(plan["batches"][0]["case_ids"], ["a", "c"])
        self.assertEqual(plan["batches"][0]["concurrency"], 2)

    def test_cross_batch_dependency_reorders(self):
        # B(P2) 依赖 A(P0)：默认优先级分批 A 已在前面
        cases = [_case("a", "A", priority="P2"), _case("b", "B", priority="P0")]
        plan = normalize_plan({"enabled": True,
                               "dependencies": [{"case_id": "b", "depends_on": ["a"]}],
                               "batching": {"mode": "priority"}}, cases, 4)
        # 重排后 A 所在批必须排在 B 所在批之前
        idx = {cid: i for i, g in enumerate(plan["batches"]) for cid in g["case_ids"]}
        self.assertLess(idx["a"], idx["b"])

    def test_manual_batch_with_per_batch_concurrency(self):
        plan = normalize_plan({"enabled": True, "batching": {"mode": "manual",
            "batches": [{"name": "第一批", "case_ids": ["a", "b"], "concurrency": 1},
                        {"name": "第二批", "case_ids": ["c"], "concurrency": 3}]}},
            self.cases, 4)
        self.assertEqual([g["name"] for g in plan["batches"]], ["第一批", "第二批"])
        self.assertEqual(plan["batches"][0]["concurrency"], 1)
        self.assertEqual(plan["batches"][1]["concurrency"], 3)


class TestOrchestratedRun(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry, self.builds, self.env_mgr, self.sched = _make_scheduler(self.tmp.name)
        self.pid = self.registry.store("projects").insert({"name": "P"})
        self.env = self.env_mgr.create(self.pid,
                                       {"name": "dev", "config": {"latency_ms": 0,
                                                                  "fail_rate": 0.0}})

    def tearDown(self):
        self.sched.shutdown()
        self.tmp.cleanup()

    def _suite(self, case_objs, orchestration):
        store = self.registry.store("cases")
        ids = [store.insert(c) for c in case_objs]
        suite = {"id": f"suite_{ids[0]}", "project_id": self.pid, "name": "S",
                 "env_id": self.env["id"], "case_ids": ids,
                 "orchestration": orchestration}
        self.registry.store("suites").insert(suite)
        return suite, ids

    def _results_by_case(self, build_id):
        results = self.builds.for_project(self.pid).results(build_id)
        return {r["case_id"]: r for r in results}

    def test_dependency_blocks_dependent_when_upstream_fails(self):
        # A 失败；B 依赖 A；C 独立 -> B blocked(原因明确)，C passed
        cases = [_case("a", "登录", fail=True), _case("b", "下单"),
                 _case("c", "健康检查")]
        suite, ids = self._suite(cases, {
            "enabled": True,
            "dependencies": [{"case_id": "b", "depends_on": ["a"]}],
            "batching": {"mode": "all", "concurrency": 3},
        })
        build = self.sched.submit_build(self.pid, suite["id"])
        final = _wait(self.builds, self.pid, build["id"], sched=self.sched)
        self.assertEqual(final["status"], "failed")

        by = self._results_by_case(build["id"])
        self.assertEqual(by["a"]["status"], "failed")
        self.assertEqual(by["c"]["status"], "passed")
        self.assertEqual(by["b"]["status"], "blocked")
        self.assertIn("依赖", by["b"]["skip_reason"])
        self.assertIn("登录", by["b"]["skip_reason"])
        self.assertEqual(final["blocked"], 1)
        self.assertEqual(final["skipped"], 0)

    def test_blocked_propagates_downstream(self):
        # A 失败 -> B 依赖 A blocked -> C 依赖 B blocked
        cases = [_case("a", "A", fail=True), _case("b", "B"), _case("c", "C")]
        suite, _ = self._suite(cases, {"enabled": True, "dependencies": [
            {"case_id": "b", "depends_on": ["a"]},
            {"case_id": "c", "depends_on": ["b"]},
        ]})
        build = self.sched.submit_build(self.pid, suite["id"])
        _wait(self.builds, self.pid, build["id"], sched=self.sched)
        by = self._results_by_case(build["id"])
        self.assertEqual(by["b"]["status"], "blocked")
        self.assertEqual(by["c"]["status"], "blocked")
        self.assertIn("阻断", by["c"]["skip_reason"])

    def test_dependent_runs_and_passes_when_upstream_passes(self):
        cases = [_case("a", "A"), _case("b", "B")]
        suite, _ = self._suite(cases, {"enabled": True,
            "dependencies": [{"case_id": "b", "depends_on": ["a"]}]})
        build = self.sched.submit_build(self.pid, suite["id"])
        final = _wait(self.builds, self.pid, build["id"], sched=self.sched)
        self.assertEqual(final["status"], "passed")
        by = self._results_by_case(build["id"])
        self.assertEqual(by["a"]["status"], "passed")
        self.assertEqual(by["b"]["status"], "passed")

    def test_setup_failure_blocks_all_and_teardown_still_runs(self):
        cases = [_case("a", "A"), _case("b", "B")]
        suite, _ = self._suite(cases, {"enabled": True,
            "setup": [{"action": "request", "method": "GET", "url": "/api/error"},
                      {"action": "assert", "type": "status",
                       "actual": "${resp.status}", "expected": 200}],
            "teardown": [{"action": "set", "key": "cleaned", "value": "1",
                          "name": "清理现场"}],
        })
        build = self.sched.submit_build(self.pid, suite["id"])
        final = _wait(self.builds, self.pid, build["id"], sched=self.sched)
        self.assertEqual(final["status"], "failed")
        self.assertEqual(final["blocked"], 2)
        by = self._results_by_case(build["id"])
        self.assertTrue(all(by[c["id"]]["status"] == "blocked" for c in cases))
        self.assertIn("setup", by["a"]["skip_reason"])
        # 后置动作即使在前置失败时也执行了（时间线里有 teardown 通过事件）
        events = self.builds.for_project(self.pid).read_timeline(build["id"])
        teardown = [e for e in events if e.get("kind") == "phase"
                    and e.get("name") == "teardown"]
        self.assertTrue(teardown)
        self.assertEqual(teardown[-1]["status"], "passed")

    def test_disabled_case_is_skipped_not_blocked(self):
        cases = [_case("a", "A", enabled=False)]
        suite, _ = self._suite(cases, {"enabled": True})
        build = self.sched.submit_build(self.pid, suite["id"])
        _wait(self.builds, self.pid, build["id"], sched=self.sched)
        by = self._results_by_case(build["id"])
        self.assertEqual(by["a"]["status"], "skipped")

    def test_priority_batches_run_in_order(self):
        cases = [_case("a", "A", priority="P2"), _case("b", "B", priority="P0")]
        suite, _ = self._suite(cases, {"enabled": True,
                                       "batching": {"mode": "priority"}})
        build = self.sched.submit_build(self.pid, suite["id"])
        _wait(self.builds, self.pid, build["id"], sched=self.sched)
        events = self.builds.for_project(self.pid).read_timeline(build["id"])
        batch_starts = [e for e in events if e["kind"] == "batch"
                        and "开始" in e["title"]]
        self.assertEqual(len(batch_starts), 2)
        self.assertIn("P0", batch_starts[0]["title"])
        self.assertIn("P2", batch_starts[1]["title"])
        # 结果上带批次号
        by = self._results_by_case(build["id"])
        self.assertEqual(by["b"]["batch"], 1)
        self.assertEqual(by["a"]["batch"], 2)

    def test_flat_run_when_orchestration_disabled(self):
        cases = [_case("a", "A"), _case("b", "B")]
        suite, _ = self._suite(cases, {"enabled": False})
        build = self.sched.submit_build(self.pid, suite["id"])
        final = _wait(self.builds, self.pid, build["id"], sched=self.sched)
        self.assertEqual(final["status"], "passed")
        self.assertEqual(final["passed"], 2)
        # 非编排构建没有时间线
        self.assertEqual(self.builds.for_project(self.pid).read_timeline(build["id"]), [])

    def test_invalid_plan_rejected_at_submit(self):
        cases = [_case("a", "A")]
        suite, _ = self._suite(cases, {"enabled": True,
            "dependencies": [{"case_id": "a", "depends_on": ["a"]}]})
        result = self.sched.submit_build(self.pid, suite["id"])
        self.assertIn("error", result)

    def test_abort_batch_blocks_remaining_in_batch(self):
        # A 失败，B 与其无依赖，on_failure=abort_batch -> B 被阻断并注明原因
        cases = [_case("a", "A", fail=True), _case("b", "B")]
        suite, _ = self._suite(cases, {"enabled": True,
                                       "on_failure": "abort_batch",
                                       "batching": {"mode": "all",
                                                    "concurrency": 1}})
        build = self.sched.submit_build(self.pid, suite["id"])
        _wait(self.builds, self.pid, build["id"], sched=self.sched)
        by = self._results_by_case(build["id"])
        self.assertEqual(by["a"]["status"], "failed")
        self.assertEqual(by["b"]["status"], "blocked")
        self.assertIn("abort_batch", by["b"]["skip_reason"])

    def test_per_batch_concurrency_is_bounded(self):
        # 两批，每批并发 1：每个用例都耗时 ~0.25s，总耗时应体现串行
        cases = []
        for i in range(4):
            c = _case(f"c{i}", f"C{i}")
            c["steps"] = [{"action": "request", "method": "GET", "url": "/api/health"},
                          {"action": "sleep", "seconds": 0.25},
                          {"action": "assert", "type": "status",
                           "actual": "${resp.status}", "expected": 200}]
            cases.append(c)
        suite, _ = self._suite(cases, {"enabled": True, "batching": {
            "mode": "manual",
            "batches": [{"name": "b1", "case_ids": ["c0", "c1"], "concurrency": 1},
                        {"name": "b2", "case_ids": ["c2", "c3"], "concurrency": 1}]}})
        build = self.sched.submit_build(self.pid, suite["id"])
        final = _wait(self.builds, self.pid, build["id"], timeout=20, sched=self.sched)
        self.assertEqual(final["status"], "passed")
        # 4 个用例各 ~0.25s 串行，至少 0.8s
        self.assertGreaterEqual(final["duration"], 0.8)

    def test_cancel_marks_unstarted_skipped_not_blocked(self):
        # 慢用例占住唯一并发槽，立刻取消；另一用例应被记为普通 skipped
        cases = [_case("a", "A"), _case("b", "B")]
        cases[0]["steps"] = [{"action": "sleep", "seconds": 5},
                             {"action": "assert", "type": "status",
                              "actual": "${resp.status}", "expected": 200}]
        suite, _ = self._suite(cases, {"enabled": True,
                                       "batching": {"mode": "all", "concurrency": 1}})
        build = self.sched.submit_build(self.pid, suite["id"])
        time.sleep(0.3)
        self.sched.cancel_build(build["id"])
        final = _wait(self.builds, self.pid, build["id"], sched=self.sched)
        self.assertEqual(final["status"], "cancelled")
        by = self._results_by_case(build["id"])
        statuses = {by["a"]["status"], by["b"]["status"]}
        self.assertIn("skipped", statuses)
        self.assertNotIn("blocked", statuses)


if __name__ == "__main__":
    unittest.main()
