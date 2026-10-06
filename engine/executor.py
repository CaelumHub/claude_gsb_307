"""测试用例执行器：步骤 + 断言 + 模拟请求 + 超时。

一个用例由若干「步骤」组成，每个步骤是下面四种之一：

- ``request``   向模拟目标发起 HTTP 请求（延迟 / 失败受环境配置影响）
- ``set``       设置变量
- ``script``    在受限命名空间里求值一个表达式，结果存入变量
- ``assert``    断言（equals / contains / regex / json_path / between ...）
- ``sleep``     等待（受取消与超时控制，分片睡眠以便及时响应取消）

执行器本身不关心并发——并发的职责在 :mod:`engine.scheduler`。这里只保证
单个用例的执行语义正确、可观测（逐步骤结果 + 断言明细 + 日志），并且
**超时 / 取消能被及时响应**（步骤之间检查截止时间与取消事件，睡眠分片）。

变量引用：步骤里的 ``actual`` / ``expected`` 等字段若写成 ``${resp.status}``
这种形式，会在求值前解析成变量表中的实际值（支持 ``a.b.c`` 点路径）。
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import time
from typing import Any, Optional

from .models import new_id


class ExecutionError(Exception):
    """执行过程中的预期内错误。"""


_SAFE_BUILTINS = {
    "abs": abs, "min": min, "max": max, "round": round, "len": len,
    "int": int, "float": float, "str": str, "bool": bool, "sum": sum,
    "sorted": sorted, "range": range, "list": list, "dict": dict,
    "True": True, "False": False, "None": None,
}

_FORBIDDEN = ("import", "__", "open", "eval", "exec", "globals", "locals",
              "getattr", "setattr", "compile", "os", "sys", "subprocess",
              "input", "breakpoint")


# ---------------------------------------------------------------------------
# 变量解析
# ---------------------------------------------------------------------------

def _lookup_path(path: str, variables: dict) -> Any:
    current: Any = variables
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return None
            current = current[part]
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


def resolve_expr(expr: Any, variables: dict) -> Any:
    """解析表达式中的 ``${...}`` 变量引用。

    - 整个表达式就是 ``${x.y}`` 时，返回其真实类型（dict/list/数值）；
    - 字符串里夹着 ``${x}`` 时，做字符串替换。
    """
    if not isinstance(expr, str):
        return expr
    m = re.fullmatch(r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}", expr.strip())
    if m:
        return _lookup_path(m.group(1), variables)
    return re.sub(
        r"\$\{([A-Za-z_][A-Za-z0-9_.]*)\}",
        lambda mm: "" if _lookup_path(mm.group(1), variables) is None
        else str(_lookup_path(mm.group(1), variables)),
        expr,
    )


# ---------------------------------------------------------------------------
# 安全求值
# ---------------------------------------------------------------------------

def safe_eval(expr: str, variables: dict) -> Any:
    """在受限命名空间里求值表达式，禁止导入 / 属性逃逸 / 危险内建。"""
    lowered = expr.lower()
    for word in _FORBIDDEN:
        if word in lowered:
            raise ExecutionError(f"表达式包含禁止的内容: {word}")
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ExecutionError(f"表达式语法错误: {exc}")

    # 拒绝属性访问与下标以外的复杂结构，防止 ``().__class__`` 逃逸
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            raise ExecutionError("表达式不允许属性访问（请用 ${变量} 引用）")
        if isinstance(node, ast.Subscript):
            # 允许简单下标读取，但拒绝下标里的属性访问
            pass

    namespace = dict(_SAFE_BUILTINS)
    namespace.update(variables)
    try:
        return eval(compile(tree, "<script>", "eval"), {"__builtins__": _SAFE_BUILTINS}, namespace)
    except ExecutionError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ExecutionError(f"表达式求值失败: {exc}")


# ---------------------------------------------------------------------------
# 模拟请求目标
# ---------------------------------------------------------------------------

def _seeded_int(*parts: Any) -> int:
    h = hashlib.md5("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    return int(h[:8], 16)


class MockTarget:
    """模拟被测目标：行为由环境配置决定，从而体现「环境隔离」。

    不同环境可以配不同的 ``latency_ms``、``fail_rate``、``base_url``，
    同一个用例在不同环境下会跑出不同的耗时甚至成败——这正是环境隔离
    要表达的东西：用例逻辑不变，但运行环境变了，结果随之变化。
    """

    def __init__(self, env_config: dict):
        self.base_url = (env_config or {}).get("base_url", "http://mock.local")
        self.latency_ms = int((env_config or {}).get("latency_ms", 20))
        self.fail_rate = float((env_config or {}).get("fail_rate", 0.0))

    def request(self, case_id: str, step_index: int, method: str,
                url: str, params: dict = None, headers: dict = None,
                body: Any = None) -> dict:
        started = time.time()
        latency = self.latency_ms + (_seeded_int(case_id, step_index, url) % 50)
        path = url.split("?")[0]

        status = 200
        payload: Any = {"ok": True, "echo": {"method": method, "url": url,
                                             "params": params or {}}}
        if "error" in path.lower() or "fail" in path.lower():
            status = 500
            payload = {"ok": False, "error": "simulated server error"}
        if "slow" in path.lower():
            latency += 600
        # 环境级失败率：确定性伪随机，同一环境同一用例结果稳定
        if self.fail_rate > 0 and _seeded_int(case_id, step_index, "fail") % 1000 < self.fail_rate * 1000:
            status = 500
            payload = {"ok": False, "error": "injected failure (env fail_rate)"}
        if "notfound" in path.lower() or status == 404:
            status = 404
            payload = {"ok": False, "error": "not found"}

        time.sleep(latency / 1000.0)
        return {
            "status": status,
            "body": payload,
            "latency_ms": round(latency, 2),
            "url": self.base_url.rstrip("/") + url,
            "duration": round(time.time() - started, 3),
        }


# ---------------------------------------------------------------------------
# 断言
# ---------------------------------------------------------------------------

ASSERT_TYPES = ("equals", "not_equals", "contains", "regex", "gt", "gte",
                "lt", "lte", "between", "in", "status", "truthy", "json_path")


def _coerce_numeric(a: Any, b: Any) -> tuple[Any, Any]:
    """比较前若双方都能转成数字，则统一转成数字（"14" 与 14 视为相等）。"""
    def _num(x: Any):
        if isinstance(x, bool):
            return None
        if isinstance(x, (int, float)):
            return x
        if isinstance(x, str):
            try:
                return float(x) if ("." in x or "e" in x.lower()) else int(x)
            except ValueError:
                return None
        return None
    na, nb = _num(a), _num(b)
    if na is not None and nb is not None:
        return na, nb
    return a, b


def evaluate_assertion(atype: str, actual: Any, expected: Any) -> tuple[bool, str]:
    """求值单个断言，返回 (是否通过, 说明)。"""
    if atype == "equals":
        a, e = _coerce_numeric(actual, expected)
        ok = a == e
        return ok, f"期望 == {expected!r}"
    if atype == "not_equals":
        a, e = _coerce_numeric(actual, expected)
        ok = a != e
        return ok, f"期望 != {expected!r}"
    if atype == "contains":
        try:
            ok = expected in actual
        except TypeError:
            ok = False
        return ok, f"期望包含 {expected!r}"
    if atype == "regex":
        ok = re.search(str(expected), str(actual)) is not None
        return ok, f"期望匹配 /{expected}/"
    if atype == "gt":
        a, e = _coerce_numeric(actual, expected)
        ok = a > e
        return ok, f"期望 > {expected!r}"
    if atype == "gte":
        a, e = _coerce_numeric(actual, expected)
        ok = a >= e
        return ok, f"期望 >= {expected!r}"
    if atype == "lt":
        a, e = _coerce_numeric(actual, expected)
        ok = a < e
        return ok, f"期望 < {expected!r}"
    if atype == "lte":
        a, e = _coerce_numeric(actual, expected)
        ok = a <= e
        return ok, f"期望 <= {expected!r}"
    if atype == "between":
        if isinstance(expected, (list, tuple)) and len(expected) == 2:
            lo, hi = expected[0], expected[1]
            a, lo2 = _coerce_numeric(actual, lo)
            _, hi2 = _coerce_numeric(actual, hi)
            ok = lo2 <= a <= hi2
            return ok, f"期望在 [{lo}, {hi}] 之间"
        a, e = _coerce_numeric(actual, expected)
        ok = a == e
        return ok, f"期望 == {expected!r}"
    if atype == "in":
        try:
            ok = actual in expected
        except TypeError:
            ok = False
        return ok, f"期望属于 {expected!r}"
    if atype == "status":
        ok = actual == expected
        return ok, f"期望状态码 == {expected!r}"
    if atype == "truthy":
        ok = bool(actual)
        return ok, "期望为真值"
    if atype == "json_path":
        # expected 是路径字符串，如 "data.items[0].id"；存在且非空即通过
        value = _lookup_path(str(expected), {"_root": actual})
        ok = value is not None
        return ok, f"期望路径 {expected!r} 存在"
    return False, f"未知断言类型 {atype!r}"


# ---------------------------------------------------------------------------
# 执行器
# ---------------------------------------------------------------------------

class TestExecutor:
    """测试用例执行器。"""

    def __init__(self):
        self.target = MockTarget({})

    # -- 单步骤执行 -------------------------------------------------------
    def _run_step(self, step: dict, variables: dict, case_id: str, idx: int,
                  cancel_event=None) -> dict:
        action = step.get("action", "assert")
        name = step.get("name") or action
        started = time.time()
        result: dict = {"index": idx, "action": action, "name": name,
                        "status": "passed", "message": "", "duration": 0.0}

        def _done(status: str, message: str) -> dict:
            result["status"] = status
            result["message"] = message
            result["duration"] = round(time.time() - started, 3)
            return result

        try:
            if action == "request":
                method = step.get("method", "GET")
                url = resolve_expr(step.get("url", "/"), variables)
                resp = self.target.request(
                    case_id, idx, method, str(url),
                    params=resolve_expr(step.get("params"), variables) or {},
                    headers=resolve_expr(step.get("headers"), variables) or {},
                    body=resolve_expr(step.get("body"), variables),
                )
                save_as = step.get("save_as") or "resp"
                variables[save_as] = resp
                return _done("passed", f"{method} {url} -> {resp['status']} ({resp['latency_ms']}ms)")

            if action == "set":
                key = step.get("key")
                value = resolve_expr(step.get("value"), variables)
                variables[key] = value
                return _done("passed", f"设置 {key} = {value!r}")

            if action == "script":
                expr = step.get("expr", "")
                value = safe_eval(expr, variables)
                save_as = step.get("save_as")
                if save_as:
                    variables[save_as] = value
                return _done("passed", f"{expr} = {value!r}")

            if action == "sleep":
                seconds = float(step.get("seconds", 0.1))
                self._interruptible_sleep(seconds, cancel_event)
                return _done("passed", f"等待 {seconds}s")

            if action == "skip":
                return _done("skipped", step.get("message", "跳过"))

            if action == "assert":
                atype = step.get("type", "equals")
                expected = resolve_expr(step.get("expected"), variables)
                actual = resolve_expr(step.get("actual"), variables)
                ok, msg = evaluate_assertion(atype, actual, expected)
                variables.setdefault("_assertions", []).append({
                    "name": name, "type": atype, "expected": expected,
                    "actual": actual, "ok": ok, "message": msg,
                })
                if not ok:
                    return _done("failed", f"{msg}，实际 {actual!r}")
                return _done("passed", f"{msg} ✓")

            return _done("error", f"未知步骤类型 {action!r}")
        except ExecutionError as exc:
            return _done("error", str(exc))
        except Exception as exc:  # noqa: BLE001
            return _done("error", f"{type(exc).__name__}: {exc}")

    @staticmethod
    def _interruptible_sleep(seconds: float, cancel_event=None) -> None:
        """分片睡眠，使取消 / 超时能及时响应。"""
        chunk = 0.05
        remaining = seconds
        while remaining > 0:
            if cancel_event is not None and cancel_event.is_set():
                raise ExecutionError("已取消")
            time.sleep(min(chunk, remaining))
            remaining -= chunk

    # -- 用例执行 ---------------------------------------------------------
    def execute_case(self, case: dict, env_config: dict = None,
                     cancel_event=None, timeout: float = None) -> dict:
        """执行一个用例，返回结构化的执行结果。"""
        env_config = env_config or {}
        case_id = case.get("id", new_id("case"))
        case_name = case.get("name", "未命名用例")
        timeout = timeout or case.get("timeout", 60)
        started = time.time()
        deadline = started + timeout

        self.target = MockTarget(env_config)
        variables = dict(env_config.get("variables", {}))
        variables["case"] = {"id": case_id, "name": case_name}

        logs: list[str] = [
            f"开始执行用例 {case_name} (id={case_id})，超时 {timeout}s",
        ]
        steps_out: list[dict] = []
        assertions: list[dict] = []
        status = "passed"

        if not case.get("enabled", True):
            return self._finalize(case, "skipped", steps_out, [], logs, started,
                                  "用例已禁用")

        steps = case.get("steps") or []
        for idx, step in enumerate(steps):
            if cancel_event is not None and cancel_event.is_set():
                status = "error"
                logs.append("用例被取消")
                break
            if time.time() > deadline:
                status = "timeout"
                logs.append(f"用例超时（>{timeout}s），在第 {idx} 步停止")
                break

            step_result = self._run_step(step, variables, case_id, idx, cancel_event)
            steps_out.append(step_result)
            logs.append(f"  步骤 {idx + 1}/{len(steps)} [{step_result['status']}] "
                        f"{step_result['name']}: {step_result['message']}")
            if step_result["status"] == "skipped":
                status = "skipped"
                break
            if step_result["status"] in ("failed", "error"):
                status = step_result["status"]
                logs.append(f"  用例因步骤 {idx + 1} 失败而终止")
                break

        # 校验超时（兜底）
        if status == "passed" and time.time() > deadline:
            status = "timeout"
            logs.append(f"用例总耗时超过 {timeout}s")

        assertions = variables.get("_assertions", [])
        return self._finalize(case, status, steps_out, assertions, logs, started)

    def _finalize(self, case: dict, status: str, steps: list, assertions: list,
                  logs: list, started: float, message: str = "") -> dict:
        return {
            "case_id": case.get("id"),
            "case_name": case.get("name", "未命名用例"),
            "group": (case.get("tags") or ["默认"])[0],
            "priority": case.get("priority", "P3"),
            "status": status,
            "duration": round(time.time() - started, 3),
            "steps": steps,
            "assertions": assertions,
            "logs": logs,
            "message": message,
        }
