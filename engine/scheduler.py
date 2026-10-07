"""并发调度：构建池 + 用例池 + 定时触发循环。

这是平台「测试调度与并发」难点的核心。一次构建要并发执行大量用例，多场
构建又要并行推进，同时定时任务到点还要自动触发新构建。三层并发：

1. **构建级并发**：一个线程池（``build_pool``）承载多场同时进行的构建，
   用 ``max_build_workers`` 限制并发构建数，避免磁盘/CPU 被打满；
2. **用例级并发**：每场构建内部再用一个线程池（``case_pool``）并发跑
   用例，用 ``max_case_workers`` 限制单构建内的并发度；结果通过
   :meth:`storage.buildstore.BuildStore.record_result` 在文件锁保护下
   并发安全地收集与聚合；
3. **定时触发**：一个后台循环线程按 ``tick`` 间隔扫描启用的定时计划，
   命中 cron 且本分钟尚未触发过就提交新构建，防止同一分钟重复触发。

取消：每个构建持有一个 ``threading.Event``，用例执行器在步骤之间检查它，
取消后已在跑或用例尽快中止、未跑的不再启动，最终构建标为 ``cancelled``。
"""

from __future__ import annotations

import datetime
import threading
import time
from concurrent.futures import (FIRST_COMPLETED, ThreadPoolExecutor,
                                as_completed, wait as futures_wait)
from typing import Optional

from .cron import cron_matches, parse_cron
from .models import new_id
from .orchestration import describe_plan


def _public_plan(plan: Optional[dict]) -> Optional[dict]:
    """存入 build.json 的编排摘要（去掉仅供运行时使用的 dep_map）。"""
    if plan is None:
        return None
    return describe_plan(plan)


class Scheduler:
    """测试并发调度器。"""

    def __init__(self, registry, build_registry, executor, env_manager,
                 report_gen, coverage_analyzer, defect_manager, notify_manager,
                 max_build_workers: int = 4, max_case_workers: int = 8,
                 tick_seconds: float = 20.0):
        self.registry = registry
        self.builds = build_registry
        self.executor = executor
        self.env_manager = env_manager
        self.report_gen = report_gen
        self.coverage = coverage_analyzer
        self.defects = defect_manager
        self.notify = notify_manager

        self.max_build_workers = max_build_workers
        self.max_case_workers = max_case_workers
        self.tick_seconds = tick_seconds

        self._build_pool = ThreadPoolExecutor(
            max_workers=max_build_workers, thread_name_prefix="build")
        self._running: dict[str, dict] = {}
        self._running_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._tick_thread: Optional[threading.Thread] = None
        self._scan_lock = threading.Lock()

    # ------------------------------------------------------------------ 启动
    def start(self) -> None:
        if self._tick_thread is None:
            self._tick_thread = threading.Thread(
                target=self._tick_loop, name="scheduler-tick", daemon=True)
            self._tick_thread.start()

    def shutdown(self) -> None:
        self._stop_event.set()
        self._build_pool.shutdown(wait=False, cancel_futures=True)

    # ------------------------------------------------------------------ 触发
    def submit_build(self, project_id: str, suite_id: str,
                     env_id: Optional[str] = None, trigger: str = "manual",
                     schedule_id: Optional[str] = None) -> dict:
        """提交一场构建，立即返回构建元信息（构建在后台线程池运行）。"""
        suites = self.registry.store("suites")
        cases_store = self.registry.store("cases")

        suite = suites.get(suite_id)
        if suite is None:
            return {"error": "测试套件不存在"}

        env_id = env_id or suite.get("env_id")
        if not env_id:
            envs = self.env_manager.list(project_id)
            if not envs:
                return {"error": "项目还没有可用环境，请先创建环境"}
            env_id = envs[0]["id"]
        if self.env_manager.get(env_id) is None:
            return {"error": "环境不存在"}

        case_ids = suite.get("case_ids") or []
        # 用例按套件顺序执行（get_many 来自分片存储，顺序不保证）
        case_by_id = {c.get("id"): c for c in cases_store.get_many(case_ids)}
        cases = [case_by_id[cid] for cid in case_ids if cid in case_by_id]
        if not cases:
            return {"error": "套件内没有用例"}

        # 编排计划（可选）：有 enabled 的 orchestration 才走编排执行路径
        from .orchestration import PlanError, is_enabled, normalize_plan
        plan = None
        raw_plan = suite.get("orchestration")
        if is_enabled(raw_plan):
            try:
                plan = normalize_plan(raw_plan, cases, self.max_case_workers)
            except PlanError as exc:
                return {"error": f"编排配置无效：{exc}"}

        build_id = new_id("build")
        build = self.builds.for_project(project_id).create(
            build_id,
            suite_id=suite_id,
            env_id=env_id,
            name=suite.get("name", ""),
            trigger=trigger,
            plan=_public_plan(plan) if plan else None,
        )

        cancel_event = threading.Event()
        with self._running_lock:
            self._running[build_id] = {
                "cancel": cancel_event,
                "project_id": project_id,
                "suite_id": suite_id,
            }

        self._build_pool.submit(
            self._run_build, project_id, build_id, cases, env_id, cancel_event,
            plan)

        # 若是定时触发，记录一次计划运行历史
        if schedule_id:
            self._record_schedule_run(schedule_id, project_id, build_id)

        return build

    def cancel_build(self, build_id: str) -> dict:
        handle = self._running.get(build_id)
        if handle is None:
            return {"error": "构建不在运行中或不存在"}
        handle["cancel"].set()
        return {"ok": True, "build_id": build_id}

    def running(self) -> list[dict]:
        out = []
        with self._running_lock:
            for build_id, handle in list(self._running.items()):
                build = self.builds.for_project(handle["project_id"]).get(build_id)
                out.append({
                    "build_id": build_id,
                    "project_id": handle["project_id"],
                    "suite_id": handle["suite_id"],
                    "status": build.get("status") if build else "running",
                    "total": build.get("total", 0) if build else 0,
                    "passed": build.get("passed", 0) if build else 0,
                    "failed": build.get("failed", 0) if build else 0,
                    "started_at": build.get("started_at") if build else None,
                })
        return out

    # ------------------------------------------------------------------ 构建执行
    def _run_build(self, project_id: str, build_id: str, cases: list,
                   env_id: str, cancel_event: threading.Event,
                   plan: Optional[dict] = None) -> None:
        if plan is not None:
            self._run_orchestrated_build(
                project_id, build_id, cases, env_id, cancel_event, plan)
            with self._running_lock:
                self._running.pop(build_id, None)
            return
        self._run_flat_build(project_id, build_id, cases, env_id, cancel_event)
        with self._running_lock:
            self._running.pop(build_id, None)

    def _run_flat_build(self, project_id: str, build_id: str, cases: list,
                        env_id: str, cancel_event: threading.Event) -> None:
        store = self.builds.for_project(project_id)
        env_config = self.env_manager.to_executor_config(env_id)
        store.set_total(build_id, len(cases))
        store.append_log(build_id, f"构建 {build_id} 开始，共 {len(cases)} 个用例，"
                                   f"环境 {env_id}")

        case_workers = max(1, min(self.max_case_workers, len(cases)))
        try:
            with ThreadPoolExecutor(max_workers=case_workers,
                                    thread_name_prefix=f"case-{build_id[:6]}") as pool:
                futures = {}
                for i, case in enumerate(cases):
                    if cancel_event.is_set():
                        break
                    futures[pool.submit(
                        self._run_one, case, env_config, env_id, i,
                        cancel_event)] = case

                for future in as_completed(futures):
                    case = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:  # noqa: BLE001
                        result = {
                            "case_id": case.get("id"),
                            "case_name": case.get("name", "未命名用例"),
                            "group": (case.get("tags") or ["默认"])[0],
                            "priority": case.get("priority", "P3"),
                            "status": "error",
                            "duration": 0.0,
                            "steps": [],
                            "assertions": [],
                            "logs": [f"用例执行异常: {exc}"],
                        }
                    self._persist_result(store, build_id, case, result)
                # 未提交的用例（被取消跳过）记为 skipped
                submitted = {case.get("id") for case in futures.values()}
                for case in cases:
                    if case.get("id") not in submitted:
                        skipped = {
                            "case_id": case.get("id"),
                            "case_name": case.get("name", "未命名用例"),
                            "group": (case.get("tags") or ["默认"])[0],
                            "priority": case.get("priority", "P3"),
                            "status": "skipped",
                            "duration": 0.0,
                            "steps": [], "assertions": [],
                            "logs": ["因取消而未执行"],
                        }
                        self._persist_result(store, build_id, case, skipped)
        except Exception as exc:  # noqa: BLE001
            store.append_log(build_id, f"构建执行异常: {exc}")

        # 终态判定
        build = store.get(build_id)
        if cancel_event.is_set():
            status = "cancelled"
        elif (build.get("failed", 0) + build.get("error", 0) + build.get("timeout", 0)) == 0:
            status = "passed"
        else:
            status = "failed"
        store.finish(build_id, status)
        store.append_log(build_id, f"构建结束: {status}（通过 {build.get('passed', 0)}"
                                   f"/{build.get('total', 0)}）")

        # 收尾：报告 + 覆盖率 + 通知 + 自动缺陷
        self._finalize(project_id, build_id)

    def _run_one(self, case: dict, env_config: dict, env_id: str,
                 index: int, cancel_event: threading.Event) -> dict:
        result = self.executor.execute_case(
            case, env_config, cancel_event=cancel_event,
            timeout=case.get("timeout", 60))
        result["env_id"] = env_id
        result["order"] = index
        return result

    def _persist_result(self, store, build_id: str, case: dict, result: dict) -> None:
        store.record_result(build_id, result)
        case_id = case.get("id")
        if case_id:
            log_text = "\n".join(result.get("logs", []))
            store.write_case_log(build_id, case_id, log_text)

    # ------------------------------------------------------------------ 编排执行
    def _run_orchestrated_build(self, project_id: str, build_id: str, cases: list,
                                env_id: str, cancel_event: threading.Event,
                                plan: dict) -> None:
        """按编排计划执行：前置动作 → 逐批（依赖门控 + 每批并发）→ 后置动作。"""
        store = self.builds.for_project(project_id)
        env_config = self.env_manager.to_executor_config(env_id)
        store.set_total(build_id, len(cases))
        store.append_log(
            build_id,
            f"构建 {build_id} 以编排模式开始：{len(plan['batches'])} 批，"
            f"{len(cases)} 个用例，依赖策略 {plan['dependency_policy']}，环境 {env_id}")
        store.append_event(build_id, "build", "构建开始（编排模式）", "running",
                           detail=f"{len(plan['batches'])} 批 / {len(cases)} 用例")

        case_by_id = {c.get("id"): c for c in cases}
        # statuses: 用例 id -> 终态字符串；reasons 记录阻断/跳过原因
        statuses: dict[str, str] = {}
        reasons: dict[str, str] = {}
        order_counter = {"n": 0}
        aborted = False  # 前置动作失败导致整场跳过

        # -- 1) 前置动作 ---------------------------------------------------
        if plan.get("setup"):
            setup = self._run_phase(store, build_id, "setup", plan["setup"],
                                    env_config, env_id, cancel_event)
            if setup["status"] != "passed":
                aborted = True
                store.append_log(build_id, "前置动作失败，全部用例不再执行，"
                                           "直接进入后置清理（若有）")

        # -- 2) 分批执行 ---------------------------------------------------
        if aborted:
            for case in cases:
                self._record_blocked(store, build_id, case, order_counter,
                                     reason="前置动作（setup）未通过")
                statuses[case.get("id")] = "blocked"
                reasons[case.get("id")] = "前置动作（setup）未通过"
        elif cancel_event.is_set():
            for case in cases:
                self._record_cancel_skipped(store, build_id, case, order_counter)
                statuses[case.get("id")] = "skipped"
        else:
            for bi, group in enumerate(plan["batches"], start=1):
                statuses, reasons, cancel_event, should_stop = self._run_batch(
                    store, build_id, group, bi, plan, case_by_id, statuses,
                    reasons, order_counter, env_config, env_id, cancel_event)
                if should_stop:
                    # 构建被取消：本批及后续批次中还没有终态的用例记为「取消跳过」
                    for gi, g in enumerate(plan["batches"][bi - 1:], start=bi):
                        for cid in g["case_ids"]:
                            if cid not in statuses and cid in case_by_id:
                                self._record_cancel_skipped(
                                    store, build_id, case_by_id[cid], order_counter,
                                    batch_index=gi)
                                statuses[cid] = "skipped"
                    break

        # -- 3) 后置动作（默认总是执行，即便取消 / 前置失败） ---------------
        if plan.get("teardown"):
            if cancel_event.is_set() and plan.get("teardown_policy", "always") == "always":
                teardown_cancel = threading.Event()  # 放行取消，保证清理跑完
                store.append_log(build_id, "构建已取消，后置清理仍按策略执行")
            else:
                teardown_cancel = cancel_event
            self._run_phase(store, build_id, "teardown", plan["teardown"],
                            env_config, env_id, teardown_cancel)

        # -- 4) 终态判定 ---------------------------------------------------
        build = store.get(build_id)
        teardown_failed = False
        if plan.get("teardown"):
            # 后置动作失败通过时间线最后状态体现；读日志代价大，这里以时间线判断
            last_teardown = [e for e in store.read_timeline(build_id)
                             if e.get("kind") == "phase" and e.get("name") == "teardown"]
            teardown_failed = bool(last_teardown and
                                   last_teardown[-1].get("status") not in ("passed",))
        if cancel_event.is_set():
            status = "cancelled"
        elif (build.get("failed", 0) + build.get("error", 0) + build.get("timeout", 0)
              + build.get("blocked", 0)) == 0 and not teardown_failed:
            status = "passed"
        else:
            status = "failed"
        store.finish(build_id, status)
        store.append_log(build_id, f"构建结束: {status}（通过 {build.get('passed', 0)}"
                                   f"/{build.get('total', 0)}，阻断 {build.get('blocked', 0)}，"
                                   f"跳过 {build.get('skipped', 0)}）")
        store.append_event(build_id, "build", f"构建结束：{status}", status,
                           detail=f"通过 {build.get('passed', 0)}/{build.get('total', 0)}")
        self._finalize(project_id, build_id)

    def _run_phase(self, store, build_id: str, phase: str, steps: list,
                   env_config: dict, env_id: str,
                   cancel_event: threading.Event) -> dict:
        """执行一个前置 / 后置阶段，并把进度逐动作写入时间线。"""
        label = "前置动作" if phase == "setup" else "后置动作"
        store.append_event(build_id, "phase", f"{label}开始", "running",
                           name=phase, detail=f"{len(steps)} 个动作")
        result = self.executor.execute_phase(
            phase, steps, env_config, cancel_event=cancel_event)
        for line in result.get("logs", []):
            store.append_log(build_id, line)
        status = result["status"]
        failing = next((s for s in result.get("steps", [])
                        if s.get("status") in ("failed", "error", "timeout")), None)
        detail = (failing or {}).get("message", "") if failing else ""
        store.append_event(build_id, "phase", f"{label}{self._phase_word(status)}",
                           status if status in ("passed", "failed", "error", "timeout")
                           else "error", name=phase, detail=detail,
                           duration=result.get("duration", 0.0))
        return result

    @staticmethod
    def _phase_word(status: str) -> str:
        return {"passed": "通过", "failed": "失败", "error": "出错",
                "timeout": "超时"}.get(status, "结束")

    def _run_batch(self, store, build_id: str, group: dict, batch_index: int,
                   plan: dict, case_by_id: dict, statuses: dict, reasons: dict,
                   order_counter: dict, env_config: dict, env_id: str,
                   cancel_event: threading.Event):
        """执行一批：批内按依赖门控放行，并发不超过该批 concurrency。

        返回 ``(statuses, reasons, cancel_event, should_stop)``，``should_stop``
        为 True 表示构建被取消，后续批次不再执行。
        """
        ids = group["case_ids"]
        concurrency = max(1, min(int(group.get("concurrency")
                                     or plan["default_concurrency"]), len(ids) or 1))
        dep_map = plan["dep_map"]
        store.append_event(
            build_id, "batch",
            f"第 {batch_index} 批「{group['name']}」开始", "running",
            batch=batch_index, detail=f"{len(ids)} 用例 · 并发 {concurrency}",
            concurrency=concurrency, size=len(ids))
        store.append_log(build_id, f"[批次 {batch_index}] {group['name']} 开始"
                                   f"（{len(ids)} 用例，并发上限 {concurrency}）")

        pending = [cid for cid in ids]          # 尚未有终态、也未提交
        in_flight: dict = {}                     # future -> case_id
        batch_failure_seen = False

        def gate(cid: str) -> str:
            """依赖门控：返回 running（继续等待）/ blocked:<原因> / ready。"""
            for dep in dep_map.get(cid, []):
                dep_st = statuses.get(dep)
                dep_name = case_by_id.get(dep, {}).get("name", dep)
                if dep_st is None:
                    # 依赖还在跑 / 在本批未提交 / 在后续批次：先等
                    return "running"
                if dep_st == "blocked":
                    return f"blocked:依赖用例「{dep_name}」因依赖未满足被阻断"
                if dep_st == "skipped":
                    if plan["on_skipped"] == "block":
                        return f"blocked:依赖用例「{dep_name}」被跳过"
                    continue
                if dep_st == "passed":
                    continue
                # failed / error / timeout
                if plan["dependency_policy"] == "passed":
                    return f"blocked:依赖用例「{dep_name}」未通过（{dep_st}）"
                if plan["dependency_policy"] == "completed":
                    continue
                # any：跑完即可
                continue
            return "ready"

        pool = ThreadPoolExecutor(max_workers=concurrency,
                                  thread_name_prefix=f"orc-{build_id[:6]}-{batch_index}")
        try:
            while pending or in_flight:
                if cancel_event.is_set():
                    break

                # 提交所有已可放行的用例（受并发上限约束）
                still_pending = []
                for cid in pending:
                    if len(in_flight) >= concurrency:
                        still_pending.append(cid)
                        continue
                    verdict = gate(cid)
                    case = case_by_id[cid]
                    if verdict == "running":
                        still_pending.append(cid)
                        continue
                    if verdict.startswith("blocked:"):
                        reason = verdict.split(":", 1)[1]
                        self._record_blocked(store, build_id, case,
                                             order_counter, reason=reason,
                                             batch_index=batch_index)
                        statuses[cid] = "blocked"
                        reasons[cid] = reason
                        store.append_event(
                            build_id, "case",
                            f"用例「{case.get('name', cid)}」依赖未满足，跳过",
                            "blocked", case_id=cid, batch=batch_index,
                            detail=reason)
                        continue
                    if not case.get("enabled", True):
                        result = self.executor.execute_case(case, env_config,
                                                            cancel_event=cancel_event)
                        self._persist_ordered(store, build_id, case, result,
                                              order_counter, batch_index)
                        statuses[cid] = result["status"]
                        store.append_event(
                            build_id, "case",
                            f"用例「{case.get('name', cid)}」已禁用，跳过",
                            "skipped", case_id=cid, batch=batch_index,
                            detail="用例被禁用（普通跳过）")
                        continue
                    if batch_failure_seen and plan["on_failure"] == "abort_batch":
                        reason = "本批已有用例失败（on_failure=abort_batch）"
                        self._record_blocked(store, build_id, case,
                                             order_counter, reason=reason,
                                             batch_index=batch_index)
                        statuses[cid] = "blocked"
                        reasons[cid] = reason
                        store.append_event(
                            build_id, "case",
                            f"用例「{case.get('name', cid)}」因本批失败被中止，跳过",
                            "blocked", case_id=cid, batch=batch_index, detail=reason)
                        continue
                    order_counter["n"] += 1
                    fut = pool.submit(self._run_one, case, env_config, env_id,
                                      order_counter["n"], cancel_event)
                    in_flight[fut] = cid
                    store.append_event(
                        build_id, "case", f"用例「{case.get('name', cid)}」开始",
                        "running", case_id=cid, batch=batch_index)
                pending = still_pending

                if not in_flight:
                    if pending:
                        # 仅剩下「等待本批依赖」的用例却没有在跑任务，理论上
                        # 不会发生（被依赖者同批时应先提交）；让出时间片兜底。
                        time.sleep(0.01)
                    continue

                done, _ = futures_wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    cid = in_flight.pop(fut)
                    case = case_by_id[cid]
                    try:
                        result = fut.result()
                    except Exception as exc:  # noqa: BLE001
                        result = {
                            "case_id": cid,
                            "case_name": case.get("name", "未命名用例"),
                            "group": (case.get("tags") or ["默认"])[0],
                            "priority": case.get("priority", "P3"),
                            "status": "error", "duration": 0.0,
                            "steps": [], "assertions": [],
                            "logs": [f"用例执行异常: {exc}"],
                        }
                    result["batch"] = batch_index
                    self._persist_result(store, build_id, case, result)
                    st = result["status"]
                    statuses[cid] = st
                    store.append_event(
                        build_id, "case",
                        f"用例「{case.get('name', cid)}」{self._case_word(st)}",
                        st if st in ("passed", "failed", "error", "timeout",
                                     "skipped", "blocked") else "error",
                        case_id=cid, batch=batch_index,
                        duration=result.get("duration", 0.0),
                        detail=result.get("message") or "")
                    if st in ("failed", "error", "timeout"):
                        batch_failure_seen = True
        finally:
            pool.shutdown(wait=True, cancel_futures=False)

        # 批结束时仍 pending 的用例（取消）：由上层统一记为取消跳过。
        remaining = [cid for cid in pending if cid not in statuses]
        should_stop = cancel_event.is_set()
        if should_stop:
            for cid in remaining:
                self._record_cancel_skipped(store, build_id, case_by_id[cid],
                                            order_counter, batch_index=batch_index)
                statuses[cid] = "skipped"
            batch_status = "cancelled"
        else:
            batch_status = "passed"
        done_count = sum(1 for cid in ids if statuses.get(cid) == "passed")
        blocked_count = sum(1 for cid in ids if statuses.get(cid) == "blocked")
        bad = sum(1 for cid in ids
                  if statuses.get(cid) in ("failed", "error", "timeout"))
        if bad:
            batch_status = "failed"
        elif blocked_count and not should_stop:
            batch_status = "failed"
        store.append_event(
            build_id, "batch",
            f"第 {batch_index} 批「{group['name']}」结束", batch_status,
            batch=batch_index,
            detail=f"通过 {done_count} · 失败 {bad} · 阻断 {blocked_count}")
        store.append_log(build_id, f"[批次 {batch_index}] {group['name']} 结束"
                                   f"（通过 {done_count}，失败 {bad}，阻断 {blocked_count}）")
        return statuses, reasons, cancel_event, should_stop

    @staticmethod
    def _case_word(status: str) -> str:
        return {"passed": "通过", "failed": "失败", "error": "出错",
                "timeout": "超时", "skipped": "跳过",
                "blocked": "依赖未满足"}.get(status, "结束")

    def _persist_ordered(self, store, build_id: str, case: dict, result: dict,
                         order_counter: dict, batch_index: int) -> None:
        order_counter["n"] += 1
        result["order"] = order_counter["n"]
        result["batch"] = batch_index
        self._persist_result(store, build_id, case, result)

    def _record_blocked(self, store, build_id: str, case: dict,
                        order_counter: dict, reason: str,
                        batch_index: Optional[int] = None) -> None:
        """依赖未满足的编排跳过：状态 blocked，带明确 skip_reason。"""
        order_counter["n"] += 1
        blocked = {
            "case_id": case.get("id"),
            "case_name": case.get("name", "未命名用例"),
            "group": (case.get("tags") or ["默认"])[0],
            "priority": case.get("priority", "P3"),
            "status": "blocked",
            "duration": 0.0,
            "steps": [], "assertions": [],
            "logs": [f"因依赖未满足而被编排跳过：{reason}"],
            "message": reason,
            "skip_reason": reason,
            "skip_kind": "dependency",
            "order": order_counter["n"],
        }
        if batch_index is not None:
            blocked["batch"] = batch_index
        store.record_result(build_id, blocked)

    def _record_cancel_skipped(self, store, build_id: str, case: dict,
                               order_counter: dict,
                               batch_index: Optional[int] = None) -> None:
        """构建被取消而未执行：普通 skipped（与 blocked 区分）。"""
        order_counter["n"] += 1
        skipped = {
            "case_id": case.get("id"),
            "case_name": case.get("name", "未命名用例"),
            "group": (case.get("tags") or ["默认"])[0],
            "priority": case.get("priority", "P3"),
            "status": "skipped",
            "duration": 0.0,
            "steps": [], "assertions": [],
            "logs": ["构建被取消，用例未执行"],
            "message": "构建被取消",
            "skip_reason": "构建被取消",
            "skip_kind": "cancelled",
            "order": order_counter["n"],
        }
        if batch_index is not None:
            skipped["batch"] = batch_index
        store.record_result(build_id, skipped)

    def _finalize(self, project_id: str, build_id: str) -> None:
        store = self.builds.for_project(project_id)
        build = store.get(build_id)
        if build is None:
            return
        total = build.get("total", 0)
        passed = build.get("passed", 0)
        passed_ratio = (passed / total) if total else 1.0

        try:
            self.report_gen.build_report(project_id, build_id, force=True)
        except Exception:  # noqa: BLE001
            pass
        try:
            self.coverage.generate(project_id, build_id, passed_ratio)
        except Exception:  # noqa: BLE001
            pass

        # 通知
        event = "build.passed" if build["status"] == "passed" else "build.failed"
        payload = {
            "build_id": build_id,
            "project_id": project_id,
            "status": build["status"],
            "passed": passed,
            "total": total,
            "pass_rate": round(passed_ratio * 100, 1),
            "duration": build.get("duration", 0.0),
        }
        self.notify.fire(project_id, "build.finished", payload)
        self.notify.fire(project_id, event, payload)

        # 自动缺陷（项目配置开启时，把失败用例转成缺陷）
        project = self.registry.store("projects").get(project_id)
        if project and project.get("auto_create_defects"):
            failures = store.results(build_id, where=[("status", "in", ["failed", "error", "timeout"])])
            for fr in failures[:20]:
                self.defects.create_from_case(project_id, fr, build_id)

    # ------------------------------------------------------------------ 定时循环
    def _tick_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._scan_schedules()
            except Exception:  # noqa: BLE001
                pass
            self._stop_event.wait(self.tick_seconds)

    def _scan_schedules(self) -> None:
        # 串行化扫描，避免多个线程（后台 tick + 手动触发）同时读到
        # 「本分钟尚未触发」而重复触发同一计划。
        with self._scan_lock:
            self._scan_schedules_locked()

    def _scan_schedules_locked(self) -> None:
        now = datetime.datetime.now()
        minute_key = now.strftime("%Y-%m-%d %H:%M")
        schedules_store = self.registry.store("schedules")
        for schedule in schedules_store.all():
            if not schedule.get("enabled", True):
                continue
            if schedule.get("last_fired_minute") == minute_key:
                continue  # 本分钟已触发过，防止同一分钟重复
            try:
                if cron_matches(schedule.get("cron", "* * * * *"), now):
                    schedule["last_fired_minute"] = minute_key
                    schedules_store.update(schedule["id"], {"last_fired_minute": minute_key})
                    self.submit_build(
                        schedule.get("project_id"),
                        schedule.get("suite_id"),
                        env_id=schedule.get("env_id"),
                        trigger="schedule",
                        schedule_id=schedule["id"],
                    )
            except ValueError:
                continue

    def _record_schedule_run(self, schedule_id: str, project_id: str,
                             build_id: str) -> None:
        self.registry.store("schedule_runs").insert({
            "id": new_id("schrun"),
            "schedule_id": schedule_id,
            "project_id": project_id,
            "build_id": build_id,
            "fired_at": time.time(),
            "status": "submitted",
        })

    def describe_cron(self, expr: str) -> str:
        """把 cron 表达式转成人话（供前端展示）。"""
        try:
            sched = parse_cron(expr)
        except ValueError:
            return "无效表达式"
        parts = []
        if sched.minute == list(range(0, 60)):
            parts.append("每分钟")
        else:
            parts.append(f"第 {','.join(map(str, sched.minute[:6]))} 分" + ("…" if len(sched.minute) > 6 else ""))
        if sched.hour != list(range(0, 24)):
            parts.append(f"{','.join(map(str, sched.hour[:6]))} 时" + ("…" if len(sched.hour) > 6 else ""))
        if sched.day != list(range(1, 32)):
            parts.append(f"每月 {','.join(map(str, sched.day[:8]))} 日" + ("…" if len(sched.day) > 8 else ""))
        if sched.weekday != list(range(0, 7)):
            parts.append(f"周 {','.join(map(str, sched.weekday))}")
        return " · ".join(parts) if parts else "每分钟"
