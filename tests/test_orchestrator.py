"""套件编排测试。

覆盖：编排校验（依赖成环 / 未知依赖 / 非法批次）、执行计划（分批、并发
默认值、计划期问题识别），以及调度器集成——前置/后置动作、依赖波次调度、
依赖未满足跳过（与普通跳过区分）、批次顺序、取消。
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import (CoverageAnalyzer, DefectManager, EnvironmentManager,
                    NotificationManager, ReportGenerator, Scheduler,
                    TestExecutor, build_plan, validate_orchestration)
from engine.orchestrator import (SKIP_DEPENDENCY, SKIP_SETUP_FAILED,
                                 SKIP_CANCELLED)
from storage import BuildStoreRegistry, StoreRegistry


def _case(cid, name=None, tags=None, priority="P2", steps=None):
    return {
        "id": cid, "name": name or cid, "priority": priority,
        "tags": tags or [], "timeout": 30, "enabled": True,
        "steps": steps if steps is not None else [
            {"action": "assert", "type": "equals", "actual": 1, "expected": 1},
        ],
    }


class TestValidateOrchestration(unittest.TestCase):
    def test_valid_config(self):
        errors = validate_orchestration({
            "setup": [{"action": "request", "url": "/setup"}],
            "teardown": [{"action": "request", "url": "/cleanup"}],
            "dependencies": {"b": ["a"]},
            "batches": [{"name": "冒烟", "tags": ["smoke"], "concurrency": 2},
                        {"name": "其余", "rest": True}],
        }, ["a", "b"])
        self.assertEqual(errors, [])

    def test_unknown_dependency(self):
        errors = validate_orchestration({"dependencies": {"b": ["ghost"]}}, ["a", "b"])
        self.assertTrue(any("套件外" in e for e in errors))

    def test_self_dependency(self):
        errors = validate_orchestration({"dependencies": {"a": ["a"]}}, ["a"])
        self.assertTrue(any("自身" in e for e in errors))

    def test_cycle_detected(self):
        errors = validate_orchestration(
            {"dependencies": {"a": ["b"], "b": ["c"], "c": ["a"]}}, ["a", "b", "c"])
        self.assertTrue(any("循环" in e for e in errors))

    def test_bad_batch(self):
        errors = validate_orchestration(
            {"batches": [{"name": "空批次"}, {"name": "x", "tags": ["t"], "concurrency": 0}]},
            ["a"])
        self.assertTrue(any("匹配不到" in e for e in errors))
        self.assertTrue(any("concurrency" in e for e in errors))

    def test_bad_hook(self):
        errors = validate_orchestration({"setup": [{"no_action": 1}]}, ["a"])
        self.assertTrue(any("前置动作" in e for e in errors))


class TestBuildPlan(unittest.TestCase):
    def test_batches_by_tags_priority_rest(self):
        cases = [
            _case("c1", tags=["smoke"]),
            _case("c2", tags=["api"], priority="P0"),
            _case("c3", tags=["api"], priority="P3"),
            _case("c4", tags=["misc"]),
        ]
        plan = build_plan(cases, {
            "batches": [
                {"name": "冒烟", "tags": ["smoke"], "concurrency": 1},
                {"name": "高优", "priorities": ["P0", "P1"]},
                {"name": "其余", "rest": True, "concurrency": 3},
            ],
        }, default_concurrency=8)
        self.assertEqual([b.name for b in plan.batches], ["冒烟", "高优", "其余"])
        self.assertEqual(plan.batches[0].case_ids, ["c1"])
        self.assertEqual(plan.batches[0].concurrency, 1)
        self.assertEqual(plan.batches[1].case_ids, ["c2"])
        self.assertEqual(plan.batches[1].concurrency, 8)  # 默认并发
        self.assertEqual(plan.batches[2].case_ids, ["c3", "c4"])
        self.assertEqual(plan.batches[2].concurrency, 3)

    def test_unmatched_cases_fall_into_fallback_batch(self):
        cases = [_case("c1", tags=["smoke"]), _case("c2", tags=["other"])]
        plan = build_plan(cases, {"batches": [{"name": "冒烟", "tags": ["smoke"]}]},
                          default_concurrency=4)
        self.assertEqual(len(plan.batches), 2)
        self.assertEqual(plan.batches[-1].case_ids, ["c2"])

    def test_no_batches_single_default_batch(self):
        cases = [_case("c1"), _case("c2")]
        plan = build_plan(cases, {}, default_concurrency=5)
        self.assertEqual(len(plan.batches), 1)
        self.assertEqual(plan.batches[0].concurrency, 5)
        self.assertEqual(sorted(plan.batches[0].case_ids), ["c1", "c2"])

    def test_cycle_members_become_problems(self):
        cases = [_case("a"), _case("b"), _case("c")]
        plan = build_plan(cases, {"dependencies": {"a": ["b"], "b": ["a"]}},
                          default_concurrency=4)
        self.assertIn("a", plan.problems)
        self.assertIn("b", plan.problems)
        self.assertNotIn("c", plan.problems)

    def test_dependency_in_later_batch_is_problem(self):
        cases = [_case("a", tags=["smoke"]), _case("b", tags=["slow"])]
        plan = build_plan(cases, {
            "dependencies": {"a": ["b"]},  # a 在第一批，却依赖第二批的 b
            "batches": [{"name": "快", "tags": ["smoke"]},
                        {"name": "慢", "rest": True}],
        }, default_concurrency=4)
        self.assertIn("a", plan.problems)
        self.assertIn("批次", plan.problems["a"])


# ---------------------------------------------------------------------------
# 调度器集成
# ---------------------------------------------------------------------------

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


class TestOrchestratedBuild(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry, self.builds, self.env_mgr, self.sched = _make_scheduler(self.tmp.name)
        self.pid = self.registry.store("projects").insert({"name": "P"})
        self.env = self.env_mgr.create(self.pid, {
            "name": "dev", "config": {"latency_ms": 0, "fail_rate": 0.0}})
        self.cases_store = self.registry.store("cases")

    def tearDown(self):
        self.sched.shutdown()
        self.tmp.cleanup()

    def _add_case(self, cid, name=None, tags=None, priority="P2", steps=None):
        case = _case(cid, name, tags, priority, steps)
        case["project_id"] = self.pid
        self.cases_store.insert(case)
        return cid

    def _add_suite(self, case_ids, orchestration=None):
        suite = {
            "id": "suite_orch", "project_id": self.pid, "name": "编排套件",
            "env_id": self.env["id"], "case_ids": case_ids,
            "orchestration": orchestration or {},
        }
        self.registry.store("suites").insert(suite)
        return suite

    def _run_and_wait(self, suite, timeout=20):
        result = self.sched.submit_build(self.pid, suite["id"], trigger="manual")
        self.assertIn("id", result)
        build_id = result["id"]
        deadline = time.time() + timeout
        build = None
        while time.time() < deadline:
            build = self.builds.for_project(self.pid).get(build_id)
            if build and build["status"] in ("passed", "failed", "cancelled", "error"):
                break
            time.sleep(0.05)
        self.assertIsNotNone(build)
        return build_id, build

    def _results(self, build_id):
        return {r["case_id"]: r
                for r in self.builds.for_project(self.pid).results(build_id)}

    def test_dependency_chain_runs_in_order(self):
        self._add_case("a", "先登录")
        self._add_case("b", "再查列表")
        self._add_case("c", "最后清理")
        suite = self._add_suite(["a", "b", "c"], {
            "dependencies": {"b": ["a"], "c": ["b"]},
        })
        build_id, build = self._run_and_wait(suite)
        self.assertEqual(build["status"], "passed")
        results = self._results(build_id)
        self.assertEqual(results["a"]["status"], "passed")
        self.assertEqual(results["b"]["status"], "passed")
        self.assertEqual(results["c"]["status"], "passed")
        # 依赖链按顺序提交：a 先于 b，b 先于 c
        self.assertLess(results["a"]["order"], results["b"]["order"])
        self.assertLess(results["b"]["order"], results["c"]["order"])

    def test_failed_dependency_skips_dependent_with_reason(self):
        self._add_case("a", "必失败", steps=[
            {"action": "assert", "type": "equals", "actual": 1, "expected": 2},
        ])
        self._add_case("b", "依赖A")
        self._add_case("c", "依赖B")
        self._add_case("d", "无依赖")
        suite = self._add_suite(["a", "b", "c", "d"], {
            "dependencies": {"b": ["a"], "c": ["b"]},
        })
        build_id, build = self._run_and_wait(suite)
        self.assertEqual(build["status"], "failed")
        results = self._results(build_id)
        self.assertEqual(results["a"]["status"], "failed")
        # b 因 a 失败而跳过，原因是依赖未满足，且写明是哪个依赖
        self.assertEqual(results["b"]["status"], "skipped")
        self.assertEqual(results["b"]["skip_reason"], SKIP_DEPENDENCY)
        self.assertIn("必失败", results["b"]["message"])
        # 依赖链传导：b 被跳过，c 也依赖未满足
        self.assertEqual(results["c"]["status"], "skipped")
        self.assertEqual(results["c"]["skip_reason"], SKIP_DEPENDENCY)
        # 无依赖的 d 正常执行
        self.assertEqual(results["d"]["status"], "passed")
        # 构建级聚合：依赖跳过单独计数
        self.assertEqual(build["dep_skipped"], 2)
        self.assertEqual(build["skipped"], 2)

    def test_setup_failure_skips_everything_but_teardown_runs(self):
        self._add_case("a")
        self._add_case("b")
        suite = self._add_suite(["a", "b"], {
            "setup": [{"action": "assert", "type": "equals", "actual": 1,
                       "expected": 2, "name": "准备数据（必失败）"}],
            "teardown": [{"action": "set", "key": "cleaned", "value": True,
                          "name": "清理现场"}],
        })
        build_id, build = self._run_and_wait(suite)
        self.assertEqual(build["status"], "error")
        results = self._results(build_id)
        for cid in ("a", "b"):
            self.assertEqual(results[cid]["status"], "skipped")
            self.assertEqual(results[cid]["skip_reason"], SKIP_SETUP_FAILED)
        # 编排步骤可见：前置失败、批次跳过、后置仍执行且通过
        orch = self.builds.for_project(self.pid).read_orchestration(build_id)
        self.assertIsNotNone(orch)
        steps = {s["id"]: s for s in orch["steps"]}
        self.assertEqual(steps["setup"]["status"], "failed")
        self.assertEqual(steps["batch.0"]["status"], "skipped")
        self.assertEqual(steps["teardown"]["status"], "passed")

    def test_teardown_failure_marks_build_error(self):
        self._add_case("a")
        suite = self._add_suite(["a"], {
            "teardown": [{"action": "assert", "type": "equals", "actual": 1,
                          "expected": 2, "name": "清理（必失败）"}],
        })
        build_id, build = self._run_and_wait(suite)
        self.assertEqual(build["status"], "error")
        results = self._results(build_id)
        self.assertEqual(results["a"]["status"], "passed")  # 用例本身是通过的
        orch = self.builds.for_project(self.pid).read_orchestration(build_id)
        steps = {s["id"]: s for s in orch["steps"]}
        self.assertEqual(steps["teardown"]["status"], "failed")

    def test_batches_run_sequentially_with_own_concurrency(self):
        for i in range(3):
            self._add_case(f"smoke_{i}", tags=["smoke"])
        for i in range(3):
            self._add_case(f"rest_{i}", tags=["heavy"])
        suite = self._add_suite(
            [f"smoke_{i}" for i in range(3)] + [f"rest_{i}" for i in range(3)],
            {"batches": [{"name": "冒烟", "tags": ["smoke"], "concurrency": 1},
                         {"name": "其余", "rest": True, "concurrency": 2}]})
        build_id, build = self._run_and_wait(suite)
        self.assertEqual(build["status"], "passed")
        results = self._results(build_id)
        smoke_orders = [results[f"smoke_{i}"]["order"] for i in range(3)]
        rest_orders = [results[f"rest_{i}"]["order"] for i in range(3)]
        # 第一批次全部先于第二批次提交
        self.assertLess(max(smoke_orders), min(rest_orders))
        # 编排步骤里有两个批次，且各自完成
        orch = self.builds.for_project(self.pid).read_orchestration(build_id)
        batches = [s for s in orch["steps"] if s["type"] == "batch"]
        self.assertEqual(len(batches), 2)
        self.assertEqual(batches[0]["concurrency"], 1)
        self.assertEqual(batches[1]["concurrency"], 2)
        self.assertTrue(all(b["status"] == "passed" for b in batches))
        self.assertEqual(batches[0]["done"], batches[0]["total"])

    def test_orchestration_steps_visible_and_logged(self):
        self._add_case("a")
        suite = self._add_suite(["a"], {
            "setup": [{"action": "set", "key": "token", "value": "x", "name": "准备令牌"}],
            "teardown": [{"action": "set", "key": "done", "value": True, "name": "清理"}],
        })
        build_id, build = self._run_and_wait(suite)
        self.assertEqual(build["status"], "passed")
        orch = self.builds.for_project(self.pid).read_orchestration(build_id)
        types = [s["type"] for s in orch["steps"]]
        self.assertIn("setup", types)
        self.assertIn("setup_step", types)
        self.assertIn("batch", types)
        self.assertIn("teardown", types)
        self.assertTrue(all(s["status"] == "passed" for s in orch["steps"]))
        # 构建日志里能看到编排推进
        logs = self.builds.for_project(self.pid).read_logs(build_id)
        text = "\n".join(logs["lines"])
        self.assertIn("[编排]", text)
        self.assertIn("前置动作", text)
        self.assertIn("后置动作", text)

    def test_cycle_dependencies_skipped_as_dependency_unmet(self):
        self._add_case("a")
        self._add_case("b")
        self._add_case("c")
        suite = self._add_suite(["a", "b", "c"], {
            "dependencies": {"a": ["b"], "b": ["a"]},
        })
        build_id, build = self._run_and_wait(suite)
        results = self._results(build_id)
        self.assertEqual(results["a"]["skip_reason"], SKIP_DEPENDENCY)
        self.assertIn("循环", results["a"]["message"])
        self.assertEqual(results["b"]["skip_reason"], SKIP_DEPENDENCY)
        self.assertEqual(results["c"]["status"], "passed")
        self.assertEqual(build["dep_skipped"], 2)

    def test_cancel_marks_remaining_as_cancelled_skip(self):
        for i in range(6):
            self._add_case(f"c{i}", steps=[{"action": "sleep", "seconds": 0.4}])
        suite = self._add_suite([f"c{i}" for i in range(6)], {
            "batches": [{"name": "全部", "rest": True, "concurrency": 2}],
        })
        result = self.sched.submit_build(self.pid, suite["id"])
        build_id = result["id"]
        time.sleep(0.15)
        self.sched.cancel_build(build_id)
        deadline = time.time() + 15
        build = None
        while time.time() < deadline:
            build = self.builds.for_project(self.pid).get(build_id)
            if build and build["status"] in ("cancelled", "passed", "failed", "error"):
                break
            time.sleep(0.05)
        self.assertEqual(build["status"], "cancelled")
        results = self._results(build_id)
        self.assertEqual(len(results), 6)
        cancelled = [r for r in results.values()
                     if r.get("skip_reason") == SKIP_CANCELLED]
        self.assertTrue(cancelled)  # 有未轮到的用例被标记为取消跳过
        # 所有取消跳过的都不是依赖跳过
        self.assertEqual(build.get("dep_skipped", 0), 0)


if __name__ == "__main__":
    unittest.main()
