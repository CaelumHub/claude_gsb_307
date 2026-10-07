"""套件编排：前置/后置动作、用例依赖、分批与并发控制。

动机
----
没有编排时，套件里的用例被平铺进同一个线程池同时发出：用例 B 依赖用例 A
先跑通（例如先登录再查列表），B 却可能在 A 之前执行，白白失败一场。编排
要解决四件事：

1. **前置 / 后置动作**（setup / teardown）：套件运行前准备数据、运行后
   清理现场。前置失败则整场构建不再浪费用例执行；后置无论成败都会执行；
2. **用例顺序依赖**：``dependencies`` 声明「用例 X 依赖用例 Y」，Y 通过
   后 X 才会被调度；Y 失败 / 被跳过，X 以 ``dependency_unmet`` 原因跳过，
   并在结果里写明是哪个依赖没满足——和普通跳过（禁用、取消）严格区分；
3. **分批与并发**：``batches`` 按标签 / 优先级把用例分批，批次之间串行、
   批次内部按各自的 ``concurrency`` 并发，便于「先冒烟、再全量」；
4. **可观测**：编排的每一步（前置、每个批次、后置）都通过
   :class:`OrchestrationTracker` 落到 ``orchestration.json`` 并追加进构建
   日志，监控页轮询即可看到编排推进的全过程。

数据结构（套件上的 ``orchestration`` 字段）::

    {
      "setup":    [步骤...],                     # 前置动作（执行器步骤语法）
      "teardown": [步骤...],                     # 后置动作（清理，必定执行）
      "hook_timeout": 120,                       # 前置/后置各自的超时（秒）
      "dependencies": {"case_b": ["case_a"]},    # B 依赖 A：A 通过才跑 B
      "batches": [
        {"name": "冒烟", "tags": ["smoke"], "concurrency": 2},
        {"name": "核心", "priorities": ["P0", "P1"], "concurrency": 4},
        {"name": "其余", "rest": true, "concurrency": 8}
      ]
    }

批次匹配规则：按声明顺序匹配，用例进入第一个命中的批次；``tags`` 与
``priorities`` 同时给出时需同时满足；``rest: true`` 接住所有剩余用例；
没有任何批次命中时，自动追加一个「其他用例」兜底批次，保证用例不丢。
"""

from __future__ import annotations

import time
from typing import Any, Optional

# 跳过原因（写进用例结果的 ``skip_reason`` 字段，监控页据此区分展示）
SKIP_DISABLED = "disabled"            # 用例被禁用
SKIP_CANCELLED = "cancelled"          # 构建被取消，未轮到执行
SKIP_DEPENDENCY = "dependency_unmet"  # 依赖未满足（前置用例未通过）
SKIP_SETUP_FAILED = "setup_failed"    # 前置动作失败，整场不再执行用例
SKIP_STEP = "step"                    # 用例内部的 skip 步骤（普通跳过）

STATUS_LABELS = {
    "passed": "通过", "failed": "失败", "error": "错误",
    "skipped": "跳过", "timeout": "超时", "cancelled": "已取消",
}

_HOOK_LABELS = {"setup": "前置动作", "teardown": "后置动作"}


class OrchestrationError(ValueError):
    """编排配置不合法。"""


# ---------------------------------------------------------------------------
# 校验
# ---------------------------------------------------------------------------

def validate_orchestration(orch: Any, case_ids: list[str]) -> list[str]:
    """校验编排配置，返回错误信息列表（空列表 = 合法）。

    保存套件时调用，把配置错误（依赖了不存在的用例、依赖成环、批次写法
    不对等）挡在运行之前；运行时 :func:`build_plan` 仍会防御性处理。
    """
    errors: list[str] = []
    if orch is None:
        return errors
    if not isinstance(orch, dict):
        return ["orchestration 必须是对象"]

    id_set = set(case_ids)

    for hook in ("setup", "teardown"):
        steps = orch.get(hook)
        if steps is None:
            continue
        label = _HOOK_LABELS[hook]
        if not isinstance(steps, list):
            errors.append(f"{label}必须是步骤数组")
            continue
        for i, step in enumerate(steps):
            if not isinstance(step, dict) or not step.get("action"):
                errors.append(f"{label}第 {i + 1} 步缺少 action")

    deps = orch.get("dependencies") or {}
    if not isinstance(deps, dict):
        errors.append("dependencies 必须是 {用例id: [依赖用例id...]} 对象")
        deps = {}
    for cid, dep_list in deps.items():
        if cid not in id_set:
            errors.append(f"依赖配置指向了套件外的用例 {cid}")
        if not isinstance(dep_list, list):
            errors.append(f"用例 {cid} 的依赖必须是数组")
            continue
        for d in dep_list:
            if d not in id_set:
                errors.append(f"用例 {cid} 依赖了套件外的用例 {d}")
            if d == cid:
                errors.append(f"用例 {cid} 不能依赖自身")

    known_deps = {
        cid: [d for d in (ds if isinstance(ds, list) else []) if d in id_set]
        for cid, ds in deps.items() if cid in id_set
    }
    for member in sorted(_find_cycle_members(known_deps, id_set)):
        errors.append(f"用例 {member} 的依赖存在循环")

    batches = orch.get("batches") or []
    if not isinstance(batches, list):
        errors.append("batches 必须是数组")
        batches = []
    for i, spec in enumerate(batches):
        if not isinstance(spec, dict):
            errors.append(f"批次 {i + 1} 必须是对象")
            continue
        conc = spec.get("concurrency")
        if conc is not None:
            try:
                if int(conc) < 1:
                    raise ValueError
            except (TypeError, ValueError):
                errors.append(f"批次「{spec.get('name') or i + 1}」的 concurrency 必须是 >= 1 的整数")
        for key in ("tags", "priorities"):
            if key in spec and not isinstance(spec[key], list):
                errors.append(f"批次「{spec.get('name') or i + 1}」的 {key} 必须是数组")
        if not spec.get("rest") and not spec.get("tags") and not spec.get("priorities"):
            errors.append(f"批次「{spec.get('name') or i + 1}」既没有标签/优先级条件，"
                          f"也不是剩余批次（rest），匹配不到任何用例")

    hook_timeout = orch.get("hook_timeout")
    if hook_timeout is not None:
        try:
            if float(hook_timeout) <= 0:
                raise ValueError
        except (TypeError, ValueError):
            errors.append("hook_timeout 必须是正数")

    return errors


# ---------------------------------------------------------------------------
# 依赖图
# ---------------------------------------------------------------------------

def _find_cycle_members(deps: dict[str, list[str]], id_set: set[str]) -> set[str]:
    """找出所有处在依赖环上的用例 id（三色 DFS）。"""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {cid: WHITE for cid in id_set}
    members: set[str] = set()

    def dfs(u: str, stack: list[str]) -> None:
        color[u] = GRAY
        stack.append(u)
        for v in deps.get(u, []):
            if v not in id_set:
                continue
            if color.get(v) == GRAY:
                members.update(stack[stack.index(v):])
            elif color.get(v) == WHITE:
                dfs(v, stack)
        stack.pop()
        color[u] = BLACK

    for cid in id_set:
        if color[cid] == WHITE:
            dfs(cid, [])
    return members


def normalize_dependencies(orch: dict, id_set: set[str]) -> dict[str, list[str]]:
    """把 dependencies 归一化成 {case_id: [有效的套件内依赖 id]}。"""
    raw = (orch or {}).get("dependencies") or {}
    out: dict[str, list[str]] = {}
    if not isinstance(raw, dict):
        return out
    for cid, dep_list in raw.items():
        if cid not in id_set or not isinstance(dep_list, list):
            continue
        valid = [d for d in dep_list if d in id_set and d != cid]
        if valid:
            out[cid] = valid
    return out


# ---------------------------------------------------------------------------
# 执行计划
# ---------------------------------------------------------------------------

class Batch:
    """一个批次：一批用例 + 该批次的并发上限。"""

    def __init__(self, name: str, case_ids: list[str], concurrency: int):
        self.name = name
        self.case_ids = case_ids
        self.concurrency = max(1, int(concurrency))

    def to_dict(self) -> dict:
        return {"name": self.name, "case_ids": list(self.case_ids),
                "concurrency": self.concurrency}


class Plan:
    """一次构建的编排执行计划。"""

    def __init__(self, setup: list, teardown: list, hook_timeout: float,
                 batches: list[Batch], dependencies: dict[str, list[str]],
                 problems: dict[str, str]):
        self.setup = setup
        self.teardown = teardown
        self.hook_timeout = hook_timeout
        self.batches = batches
        self.dependencies = dependencies
        # 计划期就能判定无法执行的用例：case_id -> 原因（依赖成环 / 依赖排在
        # 后续批次等），运行时直接以 dependency_unmet 跳过
        self.problems = problems

    @property
    def enabled(self) -> bool:
        return bool(self.setup or self.teardown or self.dependencies
                    or len(self.batches) > 1)


def _case_matches(case: dict, tags: set, priorities: set) -> bool:
    if tags and not (set(case.get("tags") or []) & tags):
        return False
    if priorities and case.get("priority") not in priorities:
        return False
    return bool(tags or priorities)


def _assign_batches(cases: list[dict], specs: list[dict],
                    default_concurrency: int) -> list[Batch]:
    """按批次声明顺序把用例分批；未命中的用例进兜底批次，保证不丢。"""
    batches: list[Batch] = []
    remaining = list(cases)
    for i, spec in enumerate(specs):
        if not isinstance(spec, dict):
            continue
        name = spec.get("name") or f"批次 {i + 1}"
        try:
            conc = int(spec.get("concurrency") or default_concurrency)
        except (TypeError, ValueError):
            conc = default_concurrency
        if spec.get("rest"):
            matched, remaining = remaining, []
        else:
            tags = set(spec.get("tags") or [])
            priorities = set(spec.get("priorities") or [])
            if not tags and not priorities:
                continue  # 空选择器不匹配任何用例（校验阶段已提示）
            matched = [c for c in remaining if _case_matches(c, tags, priorities)]
            hit = {c.get("id") for c in matched}
            remaining = [c for c in remaining if c.get("id") not in hit]
        if matched:
            batches.append(Batch(name, [c.get("id") for c in matched], conc))
    if remaining:
        batches.append(Batch("其他用例", [c.get("id") for c in remaining],
                             default_concurrency))
    return batches


def build_plan(cases: list[dict], orch: Optional[dict],
               default_concurrency: int) -> Plan:
    """根据套件编排配置与用例列表构建执行计划。

    计划期能发现的问题（依赖成环、依赖的用例排在更后面的批次）记入
    ``Plan.problems``，运行时这些用例直接以依赖未满足跳过，不再空等。
    """
    orch = orch or {}
    case_ids = [c.get("id") for c in cases]
    id_set = set(case_ids)

    setup = [s for s in (orch.get("setup") or []) if isinstance(s, dict)]
    teardown = [s for s in (orch.get("teardown") or []) if isinstance(s, dict)]
    try:
        hook_timeout = float(orch.get("hook_timeout") or 120)
    except (TypeError, ValueError):
        hook_timeout = 120.0

    dependencies = normalize_dependencies(orch, id_set)

    specs = orch.get("batches") or []
    batches = _assign_batches(cases, specs if isinstance(specs, list) else [],
                              default_concurrency)

    problems: dict[str, str] = {}
    for cid in sorted(_find_cycle_members(dependencies, id_set)):
        problems[cid] = "依赖关系存在循环，无法满足"

    batch_of: dict[str, int] = {}
    for idx, batch in enumerate(batches):
        for cid in batch.case_ids:
            batch_of[cid] = idx
    name_of = {c.get("id"): c.get("name", c.get("id")) for c in cases}
    for cid, dep_ids in dependencies.items():
        if cid in problems:
            continue
        later = [d for d in dep_ids if batch_of.get(d, -1) > batch_of.get(cid, -1)]
        if later:
            names = "、".join(f"「{name_of.get(d, d)}」" for d in later)
            problems[cid] = f"依赖用例 {names} 排在更靠后的批次，无法先满足"

    return Plan(setup, teardown, hook_timeout, batches, dependencies, problems)


# ---------------------------------------------------------------------------
# 编排步骤追踪（监控页可见）
# ---------------------------------------------------------------------------

class OrchestrationTracker:
    """把编排的每一步落盘（``orchestration.json``）并追加构建日志。

    步骤树：前置动作（含各子步骤）→ 每个批次 → 后置动作（含各子步骤）。
    监控页轮询 ``GET /api/builds/<id>/orchestration`` 即可实时看到编排推进。
    """

    def __init__(self, store, build_id: str, plan: Plan):
        self.store = store
        self.build_id = build_id
        self.steps: list[dict] = []
        self._index: dict[str, dict] = {}

        def add(step_id: str, stype: str, name: str, **extra) -> None:
            step = {"id": step_id, "type": stype, "name": name,
                    "status": "pending", "started_at": None, "duration": 0.0,
                    "message": ""}
            step.update(extra)
            self.steps.append(step)
            self._index[step_id] = step

        if plan.setup:
            add("setup", "setup", f"前置动作（{len(plan.setup)} 步）")
            for i, s in enumerate(plan.setup):
                add(f"setup.{i}", "setup_step", s.get("name") or s.get("action", f"步骤 {i + 1}"))
        for i, batch in enumerate(plan.batches):
            add(f"batch.{i}", "batch", f"批次 {i + 1} · {batch.name}",
                total=len(batch.case_ids), done=0, passed=0, failed=0,
                skipped=0, concurrency=batch.concurrency)
        if plan.teardown:
            add("teardown", "teardown", f"后置动作（{len(plan.teardown)} 步）")
            for i, s in enumerate(plan.teardown):
                add(f"teardown.{i}", "teardown_step", s.get("name") or s.get("action", f"步骤 {i + 1}"))

    # -- 内部 -------------------------------------------------------------
    def _persist(self) -> None:
        self.store.write_orchestration(self.build_id, {
            "steps": self.steps, "updated_at": time.time(),
        })

    def _log(self, line: str) -> None:
        self.store.append_log(self.build_id, f"[编排] {line}")

    # -- 生命周期 ---------------------------------------------------------
    def begin(self) -> None:
        self._persist()
        if self.steps:
            self._log(f"编排计划就绪：{self._describe()}")

    def _describe(self) -> str:
        parts = []
        if any(s["type"] == "setup" for s in self.steps):
            parts.append("前置动作")
        batches = [s for s in self.steps if s["type"] == "batch"]
        if batches:
            parts.append(f"{len(batches)} 个批次")
        if any(s["type"] == "teardown" for s in self.steps):
            parts.append("后置动作")
        return " → ".join(parts)

    def start(self, step_id: str) -> None:
        step = self._index.get(step_id)
        if step is None:
            return
        step["status"] = "running"
        step["started_at"] = time.time()
        self._persist()
        self._log(f"开始：{step['name']}")

    def finish(self, step_id: str, status: str, message: str = "") -> None:
        step = self._index.get(step_id)
        if step is None:
            return
        step["status"] = status
        step["message"] = message
        if step.get("started_at"):
            step["duration"] = round(time.time() - step["started_at"], 3)
        self._persist()
        label = STATUS_LABELS.get(status, status)
        suffix = f"：{message}" if message else ""
        self._log(f"{step['name']} {label}{suffix}")

    def progress(self, step_id: str, **counts: int) -> None:
        """更新批次进度计数（done/passed/failed/skipped）。"""
        step = self._index.get(step_id)
        if step is None:
            return
        step.update(counts)
        self._persist()

    def record(self, step_id: str, status: str, message: str = "",
               duration: float = 0.0) -> None:
        """一次性写入子步骤结果（前置/后置的子步骤是执行完后回放的）。"""
        step = self._index.get(step_id)
        if step is None:
            return
        step["status"] = status
        step["message"] = message
        step["started_at"] = step.get("started_at") or time.time()
        step["duration"] = duration
        self._persist()

    def snapshot(self) -> dict:
        return {"steps": [dict(s) for s in self.steps], "updated_at": time.time()}
