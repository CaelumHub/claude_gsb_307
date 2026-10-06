"""存储层单元测试。

覆盖：分片 JSON 存储的增删改查 / 分页 / 排序、文件锁、原子写、
以及构建结果存储在**并发写**下的计数与结果一致性（结果收集与聚合难点）。
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import (BuildStore, BuildStoreRegistry, FileLock, LockTimeout,
                     ShardedStore, StoreRegistry, atomic_write_json, read_json)


class TestFileLock(unittest.TestCase):
    def test_acquire_release(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "a", "b.json")
            with FileLock(path + ".lock"):
                self.assertTrue(os.path.exists(os.path.dirname(path + ".lock")))
            # 释放后能再次获取
            with FileLock(path + ".lock"):
                pass

    def test_timeout(self):
        with tempfile.TemporaryDirectory() as d:
            lock = FileLock(os.path.join(d, "x.lock"), timeout=0.1)
            lock.acquire()
            try:
                with self.assertRaises(LockTimeout):
                    with FileLock(os.path.join(d, "x.lock"), timeout=0.1):
                        pass
            finally:
                lock.release()


class TestAtomic(unittest.TestCase):
    def test_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "meta.json")
            atomic_write_json(path, {"a": 1, "b": [1, 2, 3]})
            self.assertEqual(read_json(path, {}), {"a": 1, "b": [1, 2, 3]})

    def test_missing_returns_default(self):
        self.assertEqual(read_json("/nonexistent/path.json", "d"), "d")


class TestShardedStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry = StoreRegistry(self.tmp.name, shard_size=5)
        self.store = self.registry.store("projects")

    def tearDown(self):
        self.tmp.cleanup()

    def test_insert_and_get(self):
        rid = self.store.insert({"name": "项目A"})
        rec = self.store.get(rid)
        self.assertEqual(rec["name"], "项目A")
        self.assertTrue(rec["id"].startswith("projects_"))

    def test_insert_many_and_query(self):
        ids = self.store.insert_many([{"name": f"p{i}", "n": i} for i in range(12)])
        self.assertEqual(len(ids), 12)
        all_records = self.store.all()
        self.assertEqual(len(all_records), 12)

    def test_query_order_pagination_no_dup(self):
        self.store.insert_many([{"n": i} for i in range(20)])
        page1 = self.store.query(order_by="n", order="asc", limit=7, offset=0)
        page2 = self.store.query(order_by="n", order="asc", limit=7, offset=7)
        page3 = self.store.query(order_by="n", order="asc", limit=7, offset=14)
        values = [r["n"] for r in page1 + page2 + page3]
        self.assertEqual(values, list(range(20)))  # 连续、不重不漏

    def test_query_with_none_value_sorts_stably(self):
        self.store.insert_many([{"n": i} for i in range(3)])
        self.store.insert_many([{"n": None}, {"n": None}])
        # 空值统一排在最后（升序），不抛错
        records = self.store.query(order_by="n", order="asc")
        self.assertEqual(len(records), 5)
        self.assertIsNone(records[-1]["n"])

    def test_update_and_delete(self):
        rid = self.store.insert({"name": "old"})
        self.store.update(rid, {"name": "new"})
        self.assertEqual(self.store.get(rid)["name"], "new")
        self.store.delete(rid)
        self.assertIsNone(self.store.get(rid))

    def test_compact_removes_tombstones(self):
        ids = self.store.insert_many([{"n": i} for i in range(10)])
        self.store.delete(ids[0])
        self.store.compact()
        self.assertEqual(len(self.store.all()), 9)

    def test_concurrent_inserts_do_not_lose(self):
        """多线程并发插入：总数必须精确等于线程数。"""
        n_threads = 16
        per_thread = 25
        errors = []

        def worker(tid):
            try:
                for i in range(per_thread):
                    self.store.insert({"tid": tid, "i": i})
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(self.store.all()), n_threads * per_thread)


class TestBuildStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.registry = BuildStoreRegistry(os.path.join(self.tmp.name, "builds"))
        self.store = self.registry.for_project("proj1")

    def tearDown(self):
        self.tmp.cleanup()

    def test_lifecycle(self):
        build = self.store.create("b1", suite_id="s1", name="冒烟")
        self.assertEqual(build["status"], "pending")
        self.store.set_total("b1", 3)
        self.store.finish("b1", "passed")
        got = self.store.get("b1")
        self.assertEqual(got["status"], "passed")
        self.assertEqual(got["total"], 3)
        self.assertGreater(got["duration"], 0)

    def test_concurrent_result_collection(self):
        """并发收集结果：计数与结果条数必须严格一致（核心难点）。"""
        total = 200
        self.store.create("b2", suite_id="s1")
        self.store.set_total("b2", total)

        def worker(start, step):
            for i in range(start, total, step):
                status = "passed" if i % 3 else "failed"
                self.store.record_result("b2", {
                    "case_id": f"case_{i}",
                    "case_name": f"用例{i}",
                    "group": "g1" if i % 2 else "g2",
                    "priority": "P2",
                    "status": status,
                    "duration": 0.01,
                    "logs": [f"case {i} done"],
                })

        n = 8
        threads = [threading.Thread(target=worker, args=(k, n)) for k in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        build = self.store.get("b2")
        self.assertEqual(build["passed"] + build["failed"], total)
        self.assertEqual(len(self.store.results("b2")), total)
        # 分组聚合正确
        self.assertEqual(build["by_group"]["g1"]["total"] + build["by_group"]["g2"]["total"], total)
        # 日志序号与写入行数一致
        logs = self.store.read_logs("b2", after=0)
        self.assertEqual(logs["next_seq"], total * 2)  # 每条结果 2 行日志

    def test_logs_incremental(self):
        self.store.create("b3")
        self.store.append_log("b3", "line1")
        self.store.append_log("b3", "line2")
        first = self.store.read_logs("b3", after=0)
        self.assertEqual(first["lines"], ["line1", "line2"])
        second = self.store.read_logs("b3", after=2)
        self.assertEqual(second["lines"], [])
        self.assertTrue(second["eof"])

    def test_results_query(self):
        self.store.create("b4")
        self.store.set_total("b4", 2)
        self.store.record_result("b4", {"case_id": "c1", "case_name": "a", "status": "passed",
                                        "group": "g", "priority": "P1", "duration": 0.1, "logs": []})
        self.store.record_result("b4", {"case_id": "c2", "case_name": "b", "status": "failed",
                                        "group": "g", "priority": "P1", "duration": 0.2, "logs": []})
        failed = self.store.results("b4", where=[("status", "eq", "failed")])
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["case_id"], "c2")

    def test_coverage_and_report_cache(self):
        self.store.create("b5")
        self.store.write_coverage("b5", {"percent": 80.0})
        self.assertEqual(self.store.read_coverage("b5")["percent"], 80.0)
        self.store.write_report("b5", {"summary": {"pass_rate": 90.0}})
        self.assertEqual(self.store.read_report("b5")["summary"]["pass_rate"], 90.0)


class TestBuildStoreRegistry(unittest.TestCase):
    def test_all_builds_across_projects(self):
        with tempfile.TemporaryDirectory() as d:
            reg = BuildStoreRegistry(os.path.join(d, "builds"))
            reg.for_project("p1").create("b1")
            reg.for_project("p2").create("b2")
            builds = reg.all_builds()
            self.assertEqual({b["id"] for b in builds}, {"b1", "b2"})
            self.assertIsNotNone(reg.find_build("b1"))


if __name__ == "__main__":
    unittest.main()
