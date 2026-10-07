"""套件编排：前置/后置动作、用例依赖、分批与每批并发。

一次套件执行不再只能把用例「平铺」并发跑，而是按一份编排计划（plan）
有序推进。计划支持四类编排能力：

1. **前置 / 后置动作（setup / teardown）**
   ``setup`` 在全部用例之前执行（准备数据、初始化现场），``teardown``
   在全部用例之后执行（清理现场）。后置动作默认「总是执行」——即使构建
   被取消或前置失败，也要尽量清理。

2. **顺序依赖（depends_on）**
   用例可声明它依赖哪些用例。被依赖者必须先跑完且满足策略，本用例才会
   被放行；否则本用例被记为 ``blocked``（依赖未满足），并带上明确原因，
   与普通的 ``skipped``（禁用 / 取消）严格区分。依赖阻断会向下游传播：
   A 失败 → B 依赖 A 被阻断 → C 依赖 B 也被阻断。

3. **分批（batches）**
   - ``all``      所有用例在同一批（默认，兼容旧行为）
   - ``priority`` 按优先级分批（P0、P1…），批次按优先级从高到低
   - ``tag``      按标签分批，每个标签一批（可指定标签顺序）
   - ``manual``   手工分批，前端给出每批要包含的用例 id
   批次严格串行：前一批全部结束后才开始下一批。依赖若跨批次，则依赖的
   批次自然排在前面（同一批内仍由依赖门控放行）。

4. **每批并发（concurrency）**
   批级 ``concurrency`` 覆盖编排级默认值，控制这一批内同时在跑的用例数。

计划在构建开始前由 :func:`normalize_plan` 校验：未知依赖、自依赖、依赖
环都会被显式拒绝（报错而不是带着坏配置运行）。
"""

from __future__ import annotations

from typing import Any, Optional

from .models import PRIORITIES

# 分批模式
BATCH_MODES = ("all", "priority", "tag", "manual")

# 依赖满足策略：依赖的用例以何种状态结束才算「依赖满足」
DEPENDENCY_POLICIES = ("passed", "completed", "any")

# 一批中有用例失败 / 出错 / 超时时，本批后续如何处理
ON_FAILURE = ("continue", "abort_batch")

# 依赖的用例被跳过时如何处理
ON_SKIPPED = ("block", "run")


class PlanError(ValueError):
    """编排计划非法（未知依赖 / 自依赖 / 依赖环 / 分批配置错误）。"""


# ---------------------------------------------------------------------------
# 校验与规范化
# ---------------------------------------------------------------------------

def _clean_steps(raw: Any) -> list[dict]:
    """前置/后置动作复用用例步骤格式；过滤空步骤。"""
    if not raw:
        return []
    if not isinstance(raw, list):
        raise PlanError("前置/后置动作必须是步骤数组")
    steps = []
    for i, step in enumerate(raw):
        if not isinstance(step, dict):
            raise PlanError(f"第 {i + 1} 个动作格式错误")
        action = step.get("action")
        if not action:
            continue
        clean = {"action": action, "name": step.get("name") or action}
        for key in ("method", "url", "save_as", "params", "headers", "body",
                    "key", "value", "expr", "type", "actual", "expected",
                    "seconds", "message"):
            if key in step:
                clean[key] = step[key]
        steps.append(clean)
    return steps


def _detect_cycle(dep_map: dict[str, list[str]]) -> Optional[list[str]]:
    """返回依赖图中的一条环（节点 id 列表），无环返回 None。"""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {node: WHITE for node in dep_map}
    stack: list[str] = []

    def visit(node: str) -> Optional[list[str]]:
        color[node] = GRAY
        stack.append(node)
        for dep in dep_map.get(node, []):
            if dep not in color:
                continue
            if color[dep] == GRAY:
                idx = stack.index(dep)
                return stack[idx:] + [dep]
            if color[dep] == WHITE:
                found = visit(dep)
                if found:
                    return found
        stack.pop()
        color[node] = BLACK
        return None

    for node in dep_map:
        if color[node] == WHITE:
            found = visit(node)
            if found:
                return found
    return None


def normalize_plan(raw: dict | None, cases: list[dict],
                   default_concurrency: int) -> dict:
    """把套件里的 ``orchestration`` 原始配置校验并规范化成可执行计划。

    :param cases: 套件内用例（顺序即套件顺序），每个含 id/name/tags/priority
    :param default_concurrency: 未显式配置时的每批并发上限
    :raises PlanError: 配置非法
    """
    raw = raw or {}
    if not isinstance(raw, dict):
        raise PlanError("编排配置必须是对象")

    ordered_cases = list(cases)
    case_ids = [c.get("id") for c in ordered_cases]
    id_set = {cid for cid in case_ids if cid}
    case_by_id = {c.get("id"): c for c in ordered_cases}

    # -- 依赖 -------------------------------------------------------------
    raw_deps = raw.get("dependencies") or []
    if not isinstance(raw_deps, list):
        raise PlanError("dependencies 必须是数组")
    dep_map: dict[str, list[str]] = {cid: [] for cid in case_ids}
    for entry in raw_deps:
        if not isinstance(entry, dict):
            raise PlanError("依赖项必须是 {case_id, depends_on} 对象")
        cid = entry.get("case_id")
        deps = entry.get("depends_on") or []
        if isinstance(deps, str):
            deps = [deps]
        if not isinstance(deps, list):
            raise PlanError(f"用例 {cid} 的 depends_on 必须是数组")
        if cid not in id_set:
            raise PlanError(f"依赖声明引用了不在套件内的用例: {cid}")
        cleaned = []
        for dep in deps:
            if dep == cid:
                raise PlanError(f"用例 {cid} 不能依赖自身")
            if dep not in id_set:
                raise PlanError(f"用例 {cid} 依赖了不在套件内的用例: {dep}")
            if dep not in cleaned:
                cleaned.append(dep)
        dep_map[cid] = cleaned

    cycle = _detect_cycle(dep_map)
    if cycle:
        names = [case_by_id.get(n, {}).get("name", n) for n in cycle]
        raise PlanError("用例依赖存在环: " + " -> ".join(names))

    policy = raw.get("dependency_policy", "passed")
    if policy not in DEPENDENCY_POLICIES:
        raise PlanError(f"未知依赖满足策略: {policy}")
    on_skipped = raw.get("on_skipped", "block")
    if on_skipped not in ON_SKIPPED:
        raise PlanError(f"未知 on_skipped 策略: {on_skipped}")
    on_failure = raw.get("on_failure", "continue")
    if on_failure not in ON_FAILURE:
        raise PlanError(f"未知 on_failure 策略: {on_failure}")

    # -- 分批 -------------------------------------------------------------
    batching = raw.get("batching") or {}
    if not isinstance(batching, dict):
        raise PlanError("batching 必须是对象")
    mode = batching.get("mode", "all")
    if mode not in BATCH_MODES:
        raise PlanError(f"未知分批模式: {mode}")

    try:
        global_conc = int(batching.get("concurrency") or default_concurrency)
    except (TypeError, ValueError):
        raise PlanError("每批并发必须是整数")
    global_conc = max(1, global_conc)

    raw_batches = batching.get("batches") or []
    if mode == "manual" and not isinstance(raw_batches, list):
        raise PlanError("手工分批时 batches 必须是数组")

    groups: list[dict] = []
    if mode == "all":
        groups = [{"name": "全部用例", "case_ids": list(case_ids),
                   "concurrency": global_conc}]
    elif mode == "priority":
        for pr in PRIORITIES:
            ids = [cid for cid in case_ids
                   if (case_by_id[cid].get("priority") or "P3") == pr]
            if ids:
                groups.append({"name": f"优先级 {pr}", "case_ids": ids,
                               "concurrency": global_conc})
        # 未使用标准优先级的用例兜底一批，保证不丢
        known = {cid for g in groups for cid in g["case_ids"]}
        rest = [cid for cid in case_ids if cid not in known]
        if rest:
            groups.append({"name": "其他优先级", "case_ids": rest,
                           "concurrency": global_conc})
    elif mode == "tag":
        order = batching.get("tag_order") or []
        tags_order = [t for t in order if isinstance(t, str)]
        seen: set[str] = set()
        for tag in tags_order:
            ids = [cid for cid in case_ids
                   if tag in (case_by_id[cid].get("tags") or [])]
            if ids:
                groups.append({"name": f"标签 {tag}", "case_ids": ids,
                               "concurrency": global_conc})
                seen.update(ids)
        # 其余标签按首次出现顺序补齐；一个用例归入它的第一个未处理标签
        tagged: dict[str, list[str]] = {}
        for cid in case_ids:
            if cid in seen:
                continue
            for tag in (case_by_id[cid].get("tags") or []):
                tagged.setdefault(tag, []).append(cid)
                break
            else:
                tagged.setdefault("未分组", []).append(cid)
        for tag, ids in tagged.items():
            groups.append({"name": f"标签 {tag}", "case_ids": ids,
                           "concurrency": global_conc})
    else:  # manual
        assigned: set[str] = set()
        for i, g in enumerate(raw_batches):
            if not isinstance(g, dict):
                raise PlanError(f"第 {i + 1} 批配置格式错误")
            ids = [cid for cid in (g.get("case_ids") or []) if cid in id_set]
            unknown = [cid for cid in (g.get("case_ids") or []) if cid not in id_set]
            if unknown:
                raise PlanError(f"批次「{g.get('name', i + 1)}」引用了不在套件内的用例: {unknown}")
            dup = [cid for cid in ids if cid in assigned]
            if dup:
                raise PlanError(f"用例 {dup} 被分到了多个批次")
            assigned.update(ids)
            conc = g.get("concurrency") or global_conc
            groups.append({
                "name": g.get("name") or f"第 {i + 1} 批",
                "case_ids": ids,
                "concurrency": max(1, int(conc)),
            })
        # 未手工分配的用例兜底成最后一批，保证不丢
        rest = [cid for cid in case_ids if cid not in assigned]
        if rest:
            groups.append({"name": "未分批", "case_ids": rest,
                           "concurrency": global_conc})

    # 跨批次依赖需要把「被依赖者所在批」排到前面。反复交换逆序批，直到
    # 不存在「依赖在更后批」的情况；若交换不动则说明存在跨批环（理论上
    # 上面的环检测已覆盖，这里再兜一层）。
    batch_index = {}
    for bi, g in enumerate(groups):
        for cid in g["case_ids"]:
            batch_index[cid] = bi
    groups = _order_groups_by_deps(groups, dep_map)

    # 每批内部做拓扑序，保证无依赖者/被依赖者的提交顺序合理（并发门控
    # 仍在运行时做，这里只影响提交与展示顺序）。
    for g in groups:
        g["case_ids"] = _topo_sort(g["case_ids"], dep_map)

    return {
        "enabled": bool(raw.get("enabled")),
        "setup": _clean_steps(raw.get("setup")),
        "teardown": _clean_steps(raw.get("teardown")),
        "teardown_policy": raw.get("teardown_policy", "always"),
        "dependency_policy": policy,
        "on_skipped": on_skipped,
        "on_failure": on_failure,
        "mode": mode,
        "default_concurrency": global_conc,
        "batches": groups,
        "dependencies": [{"case_id": cid, "depends_on": dep_map[cid]}
                         for cid in case_ids if dep_map[cid]],
        "dep_map": dep_map,
    }


def _order_groups_by_deps(groups: list[dict],
                          dep_map: dict[str, list[str]]) -> list[dict]:
    """若 dep 所在批排在 node 所在批之后，则交换两批，迭代到稳定。"""
    groups = [dict(g) for g in groups]
    for _ in range(len(groups) * len(groups) + 1):
        index = {}
        for bi, g in enumerate(groups):
            for cid in g["case_ids"]:
                index[cid] = bi
        swapped = False
        for cid, deps in dep_map.items():
            for dep in deps:
                if index[dep] > index[cid]:
                    groups[index[dep]], groups[index[cid]] = (
                        groups[index[cid]], groups[index[dep]])
                    swapped = True
                    break
            if swapped:
                break
        if not swapped:
            return groups
    # 迭代上限仍不稳定：放弃重排，交给运行时跨批门控（阻断）兜底
    return groups


def _topo_sort(ids: list[str], dep_map: dict[str, list[str]]) -> list[str]:
    id_set = set(ids)
    visited: set[str] = set()
    out: list[str] = []

    def visit(node: str) -> None:
        if node in visited:
            return
        visited.add(node)
        for dep in dep_map.get(node, []):
            if dep in id_set:
                visit(dep)
        out.append(node)

    for cid in ids:
        visit(cid)
    return out


def is_enabled(raw: dict | None) -> bool:
    return bool(raw and raw.get("enabled"))


def describe_plan(plan: dict) -> dict:
    """给前端 / 监控页展示的精简摘要（去掉内部 dep_map）。"""
    return {
        "enabled": plan.get("enabled", False),
        "mode": plan.get("mode", "all"),
        "dependency_policy": plan.get("dependency_policy", "passed"),
        "on_failure": plan.get("on_failure", "continue"),
        "on_skipped": plan.get("on_skipped", "block"),
        "setup_count": len(plan.get("setup") or []),
        "teardown_count": len(plan.get("teardown") or []),
        "batches": [
            {"name": g["name"], "size": len(g["case_ids"]),
             "concurrency": g["concurrency"],
             "case_ids": g["case_ids"]}
            for g in plan.get("batches", [])
        ],
        "dependencies": plan.get("dependencies", []),
    }
