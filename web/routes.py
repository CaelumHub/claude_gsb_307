"""Flask API 路由。

把平台的测试执行引擎、并发调度、结果收集、报告、覆盖率、缺陷、环境、
定时任务、通知等能力暴露为 REST 接口，前端 10 个页面通过 ``fetch`` 调用。

所有实体（项目 / 用例 / 套件 / 缺陷 / 环境 / 计划 / 集成）以 JSON 分片
存储，构建结果按「项目 + 构建」二次分片存储；写路径全部走文件锁 +
原子替换，多 worker 并发下不丢、不错位。
"""

from __future__ import annotations

import time
from typing import Optional

from flask import Blueprint, current_app, jsonify, request

from engine import new_id
from engine.executor import TestExecutor

api = Blueprint("api", __name__, url_prefix="/api")


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def _registry():
    return current_app.config["STORE_REGISTRY"]


def _builds():
    return current_app.config["BUILD_REGISTRY"]


def _scheduler():
    return current_app.config["SCHEDULER"]


def _env_mgr():
    return current_app.config["ENV_MANAGER"]


def _report():
    return current_app.config["REPORT_GEN"]


def _coverage():
    return current_app.config["COVERAGE"]


def _defects():
    return current_app.config["DEFECTS"]


def _notify():
    return current_app.config["NOTIFY"]


def _payload() -> dict:
    return request.get_json(silent=True) or {}


def _err(msg: str, code: int = 400):
    return jsonify({"error": msg}), code


def _store(name: str):
    return _registry().store(name)


def _build_or_404(build_id: str):
    build = _builds().find_build(build_id)
    if build is None:
        return None, _err("构建不存在", 404)
    return build, None


# ---------------------------------------------------------------------------
# 项目
# ---------------------------------------------------------------------------

@api.get("/projects")
def list_projects():
    projects = _store("projects").all()
    # 附上每项目的用例 / 套件 / 构建 / 缺陷计数
    cases_store = _store("cases")
    suites_store = _store("suites")
    out = []
    for p in projects:
        pid = p["id"]
        p = dict(p)
        p["case_count"] = len(cases_store.query(where=[("project_id", "eq", pid)]))
        p["suite_count"] = len(suites_store.query(where=[("project_id", "eq", pid)]))
        p["build_count"] = len(_builds().for_project(pid).list_builds())
        out.append(p)
    return jsonify({"projects": out})


@api.post("/projects")
def create_project():
    data = _payload()
    name = (data.get("name") or "").strip()
    if not name:
        return _err("项目名称不能为空")
    project = {
        "id": new_id("proj"),
        "name": name,
        "description": data.get("description", ""),
        "repo_url": data.get("repo_url", ""),
        "auto_create_defects": bool(data.get("auto_create_defects", False)),
        "created_at": time.time(),
    }
    _store("projects").insert(project)
    return jsonify(project)


@api.get("/projects/<project_id>")
def get_project(project_id: str):
    project = _store("projects").get(project_id)
    if project is None:
        return _err("项目不存在", 404)
    return jsonify(project)


@api.put("/projects/<project_id>")
def update_project(project_id: str):
    project = _store("projects").get(project_id)
    if project is None:
        return _err("项目不存在", 404)
    data = _payload()
    patch = {k: data[k] for k in ("name", "description", "repo_url",
                                  "auto_create_defects") if k in data}
    updated = _store("projects").update(project_id, patch)
    return jsonify(updated)


@api.delete("/projects/<project_id>")
def delete_project(project_id: str):
    _store("projects").delete(project_id)
    return jsonify({"ok": True})


@api.get("/projects/<project_id>/stats")
def project_stats(project_id: str):
    cases = _store("cases").query(where=[("project_id", "eq", project_id)])
    suites = _store("suites").query(where=[("project_id", "eq", project_id)])
    builds = _builds().for_project(project_id).list_builds()
    total_passed = sum(b.get("passed", 0) for b in builds)
    total_cases = sum(b.get("total", 0) for b in builds)
    return jsonify({
        "cases": len(cases),
        "suites": len(suites),
        "builds": len(builds),
        "defects": _defects().stats(project_id),
        "environments": len(_env_mgr().list(project_id)),
        "total_passed": total_passed,
        "total_cases": total_cases,
        "latest_pass_rate": _report().project_report(project_id, limit=1).get("builds", [{}])[0].get("pass_rate", 0) if builds else 0,
    })


# ---------------------------------------------------------------------------
# 测试用例
# ---------------------------------------------------------------------------

@api.get("/projects/<project_id>/cases")
def list_cases(project_id: str):
    where = [("project_id", "eq", project_id)]
    q = request.args.get("q")
    tag = request.args.get("tag")
    priority = request.args.get("priority")
    if tag:
        where.append(("tags", "contains", tag))
    if priority:
        where.append(("priority", "eq", priority))
    cases = _store("cases").query(where=where, order_by="created_at", order="desc")
    if q:
        q = q.lower()
        cases = [c for c in cases if q in (c.get("name", "") + c.get("description", "")).lower()]
    return jsonify({"cases": cases})


@api.post("/projects/<project_id>/cases")
def create_case(project_id: str):
    data = _payload()
    name = (data.get("name") or "").strip()
    if not name:
        return _err("用例名称不能为空")
    steps = data.get("steps") or []
    if not isinstance(steps, list):
        return _err("steps 必须是数组")
    case = {
        "id": new_id("case"),
        "project_id": project_id,
        "name": name,
        "description": data.get("description", ""),
        "priority": data.get("priority", "P2"),
        "tags": data.get("tags") or [],
        "timeout": int(data.get("timeout", 60)),
        "enabled": bool(data.get("enabled", True)),
        "steps": steps,
        "created_at": time.time(),
    }
    _store("cases").insert(case)
    return jsonify(case)


@api.get("/cases/<case_id>")
def get_case(case_id: str):
    case = _store("cases").get(case_id)
    if case is None:
        return _err("用例不存在", 404)
    return jsonify(case)


@api.put("/cases/<case_id>")
def update_case(case_id: str):
    case = _store("cases").get(case_id)
    if case is None:
        return _err("用例不存在", 404)
    data = _payload()
    patch = {}
    for k in ("name", "description", "priority", "tags", "timeout", "enabled", "steps"):
        if k in data:
            patch[k] = data[k]
    updated = _store("cases").update(case_id, patch)
    return jsonify(updated)


@api.delete("/cases/<case_id>")
def delete_case(case_id: str):
    _store("cases").delete(case_id)
    return jsonify({"ok": True})


@api.post("/cases/<case_id>/run")
def run_single_case(case_id: str):
    """单条用例试跑（同步执行，立即返回结果）。"""
    case = _store("cases").get(case_id)
    if case is None:
        return _err("用例不存在", 404)
    data = _payload()
    env_config = _env_mgr().to_executor_config(data.get("env_id")) if data.get("env_id") else {}
    result = TestExecutor().execute_case(case, env_config,
                                         timeout=case.get("timeout", 60))
    return jsonify(result)


# ---------------------------------------------------------------------------
# 测试套件与分组
# ---------------------------------------------------------------------------

@api.get("/projects/<project_id>/suites")
def list_suites(project_id: str):
    suites = _store("suites").query(where=[("project_id", "eq", project_id)],
                                    order_by="created_at", order="desc")
    cases_store = _store("cases")
    for s in suites:
        s["case_count"] = len(s.get("case_ids") or [])
        # 附上套件内的用例概要，便于前端直接展示
        s["cases"] = [{"id": c.get("id"), "name": c.get("name"),
                       "priority": c.get("priority"), "tags": c.get("tags")}
                      for c in cases_store.get_many(s.get("case_ids") or [])]
    return jsonify({"suites": suites})


@api.post("/projects/<project_id>/suites")
def create_suite(project_id: str):
    data = _payload()
    name = (data.get("name") or "").strip()
    if not name:
        return _err("套件名称不能为空")
    suite = {
        "id": new_id("suite"),
        "project_id": project_id,
        "name": name,
        "description": data.get("description", ""),
        "group": data.get("group", ""),
        "case_ids": data.get("case_ids") or [],
        "env_id": data.get("env_id"),
        "created_at": time.time(),
    }
    _store("suites").insert(suite)
    return jsonify(suite)


@api.get("/suites/<suite_id>")
def get_suite(suite_id: str):
    suite = _store("suites").get(suite_id)
    if suite is None:
        return _err("套件不存在", 404)
    return jsonify(suite)


@api.put("/suites/<suite_id>")
def update_suite(suite_id: str):
    suite = _store("suites").get(suite_id)
    if suite is None:
        return _err("套件不存在", 404)
    data = _payload()
    patch = {k: data[k] for k in ("name", "description", "group", "case_ids", "env_id")
             if k in data}
    updated = _store("suites").update(suite_id, patch)
    return jsonify(updated)


@api.delete("/suites/<suite_id>")
def delete_suite(suite_id: str):
    _store("suites").delete(suite_id)
    return jsonify({"ok": True})


@api.get("/projects/<project_id>/groups")
def list_groups(project_id: str):
    """从用例标签聚合出分组及每组用例数。"""
    cases = _store("cases").query(where=[("project_id", "eq", project_id)])
    groups: dict[str, int] = {}
    for c in cases:
        tags = c.get("tags") or []
        if not tags:
            groups["默认"] = groups.get("默认", 0) + 1
        else:
            for t in tags:
                groups[t] = groups.get(t, 0) + 1
    return jsonify({"groups": [{"name": k, "count": v} for k, v in sorted(groups.items())]})


# ---------------------------------------------------------------------------
# 执行：触发 / 监控 / 取消
# ---------------------------------------------------------------------------

@api.post("/suites/<suite_id>/run")
def run_suite(suite_id: str):
    suite = _store("suites").get(suite_id)
    if suite is None:
        return _err("套件不存在", 404)
    data = _payload()
    result = _scheduler().submit_build(
        suite.get("project_id"), suite_id,
        env_id=data.get("env_id"),
        trigger=data.get("trigger", "manual"),
    )
    if "id" not in result:
        return _err(result.get("error", "提交构建失败"))
    return jsonify(result)


@api.post("/builds/<build_id>/cancel")
def cancel_build(build_id: str):
    result = _scheduler().cancel_build(build_id)
    if "error" in result:
        return _err(result["error"], 404)
    return jsonify(result)


@api.get("/builds")
def list_builds():
    project_id = request.args.get("project_id")
    if project_id:
        builds = _builds().for_project(project_id).list_builds()
    else:
        builds = _builds().all_builds()
    return jsonify({"builds": builds})


@api.get("/builds/running")
def list_running():
    return jsonify({"running": _scheduler().running()})


@api.get("/builds/<build_id>")
def get_build(build_id: str):
    build, err = _build_or_404(build_id)
    if err:
        return err
    return jsonify(build)


@api.get("/builds/<build_id>/results")
def build_results(build_id: str):
    build, err = _build_or_404(build_id)
    if err:
        return err
    store = _builds().for_project(build["project_id"])
    where = []
    status = request.args.get("status")
    case_id = request.args.get("case_id")
    if status:
        where.append(("status", "eq", status))
    if case_id:
        where.append(("case_id", "eq", case_id))
    order = request.args.get("order", "asc")
    limit = request.args.get("limit", type=int)
    offset = request.args.get("offset", 0, type=int)
    records = store.results(build_id, where=where or None,
                            order_by=request.args.get("order_by") or "order",
                            order=order, limit=limit, offset=offset)
    return jsonify({"build_id": build_id, "count": len(records), "results": records})


@api.get("/builds/<build_id>/logs")
def build_logs(build_id: str):
    build, err = _build_or_404(build_id)
    if err:
        return err
    store = _builds().for_project(build["project_id"])
    after = request.args.get("after", 0, type=int)
    return jsonify(store.read_logs(build_id, after=after))


@api.get("/builds/<build_id>/cases/<case_id>/log")
def case_log(build_id: str, case_id: str):
    build, err = _build_or_404(build_id)
    if err:
        return err
    store = _builds().for_project(build["project_id"])
    return jsonify({"case_id": case_id, "log": store.read_case_log(build_id, case_id)})


@api.delete("/builds/<build_id>")
def delete_build(build_id: str):
    build, err = _build_or_404(build_id)
    if err:
        return err
    _builds().for_project(build["project_id"]).delete(build_id)
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# 测试报告
# ---------------------------------------------------------------------------

@api.get("/builds/<build_id>/report")
def build_report(build_id: str):
    build, err = _build_or_404(build_id)
    if err:
        return err
    force = request.args.get("force") == "1"
    report = _report().build_report(build["project_id"], build_id, force=force)
    if "error" in report:
        return _err(report["error"], 404)
    return jsonify(report)


@api.get("/projects/<project_id>/reports")
def project_reports(project_id: str):
    limit = request.args.get("limit", 20, type=int)
    return jsonify(_report().project_report(project_id, limit=limit))


# ---------------------------------------------------------------------------
# 代码覆盖率
# ---------------------------------------------------------------------------

@api.get("/builds/<build_id>/coverage")
def build_coverage(build_id: str):
    build, err = _build_or_404(build_id)
    if err:
        return err
    cov = _coverage().get(build["project_id"], build_id)
    return jsonify(cov)


@api.get("/projects/<project_id>/coverage/trend")
def coverage_trend(project_id: str):
    return jsonify(_coverage().trend(project_id))


# ---------------------------------------------------------------------------
# 缺陷跟踪
# ---------------------------------------------------------------------------

@api.get("/projects/<project_id>/defects")
def list_defects(project_id: str):
    defects = _defects().list(project_id,
                              status=request.args.get("status"),
                              severity=request.args.get("severity"))
    return jsonify({"defects": defects})


@api.get("/projects/<project_id>/defects/stats")
def defects_stats(project_id: str):
    return jsonify(_defects().stats(project_id))


@api.post("/projects/<project_id>/defects")
def create_defect(project_id: str):
    data = _payload()
    if not (data.get("title") or "").strip():
        return _err("缺陷标题不能为空")
    return jsonify(_defects().create(project_id, data))


@api.get("/defects/<defect_id>")
def get_defect(defect_id: str):
    defect = _defects().get(defect_id)
    if defect is None:
        return _err("缺陷不存在", 404)
    return jsonify(defect)


@api.put("/defects/<defect_id>")
def update_defect(defect_id: str):
    defect = _defects().get(defect_id)
    if defect is None:
        return _err("缺陷不存在", 404)
    data = _payload()
    patch = {k: data[k] for k in ("title", "description", "severity", "status",
                                  "assignee", "tags") if k in data}
    return jsonify(_defects().update(defect_id, patch))


@api.delete("/defects/<defect_id>")
def delete_defect(defect_id: str):
    _defects().delete(defect_id)
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# 环境管理
# ---------------------------------------------------------------------------

@api.get("/projects/<project_id>/environments")
def list_environments(project_id: str):
    return jsonify({"environments": _env_mgr().list(project_id)})


@api.post("/projects/<project_id>/environments")
def create_environment(project_id: str):
    data = _payload()
    if not (data.get("name") or "").strip():
        return _err("环境名称不能为空")
    return jsonify(_env_mgr().create(project_id, data))


@api.get("/environments/<env_id>")
def get_environment(env_id: str):
    env = _env_mgr().get(env_id)
    if env is None:
        return _err("环境不存在", 404)
    return jsonify(env)


@api.put("/environments/<env_id>")
def update_environment(env_id: str):
    env = _env_mgr().get(env_id)
    if env is None:
        return _err("环境不存在", 404)
    data = _payload()
    patch = {k: data[k] for k in ("name", "description", "python_version",
                                  "base_image", "variables", "dependencies", "config")
             if k in data}
    return jsonify(_env_mgr().update(env_id, patch))


@api.delete("/environments/<env_id>")
def delete_environment(env_id: str):
    _env_mgr().delete(env_id)
    return jsonify({"ok": True})


@api.get("/environments/<env_id>/resolve")
def resolve_environment(env_id: str):
    return jsonify(_env_mgr().resolve(env_id))


# ---------------------------------------------------------------------------
# 定时任务与触发
# ---------------------------------------------------------------------------

@api.get("/projects/<project_id>/schedules")
def list_schedules(project_id: str):
    schedules = _store("schedules").query(where=[("project_id", "eq", project_id)],
                                          order_by="created_at", order="desc")
    for s in schedules:
        s["description"] = _scheduler().describe_cron(s.get("cron", ""))
        runs = _store("schedule_runs").query(where=[("schedule_id", "eq", s["id"])],
                                             order_by="fired_at", order="desc", limit=10)
        s["recent_runs"] = runs
    return jsonify({"schedules": schedules})


@api.post("/projects/<project_id>/schedules")
def create_schedule(project_id: str):
    data = _payload()
    cron = (data.get("cron") or "").strip()
    from engine.cron import parse_cron
    try:
        parse_cron(cron)
    except ValueError as exc:
        return _err(str(exc))
    schedule = {
        "id": new_id("sch"),
        "project_id": project_id,
        "name": data.get("name", "定时任务"),
        "cron": cron,
        "suite_id": data.get("suite_id"),
        "env_id": data.get("env_id"),
        "enabled": bool(data.get("enabled", True)),
        "last_fired_minute": None,
        "created_at": time.time(),
    }
    _store("schedules").insert(schedule)
    schedule["description"] = _scheduler().describe_cron(cron)
    return jsonify(schedule)


@api.put("/schedules/<schedule_id>")
def update_schedule(schedule_id: str):
    sched = _store("schedules").get(schedule_id)
    if sched is None:
        return _err("定时任务不存在", 404)
    data = _payload()
    patch = {k: data[k] for k in ("name", "cron", "suite_id", "env_id", "enabled")
             if k in data}
    if "cron" in patch:
        from engine.cron import parse_cron
        try:
            parse_cron(patch["cron"])
        except ValueError as exc:
            return _err(str(exc))
        patch["last_fired_minute"] = None  # 修改表达式后重置触发标记
    return jsonify(_store("schedules").update(schedule_id, patch))


@api.delete("/schedules/<schedule_id>")
def delete_schedule(schedule_id: str):
    _store("schedules").delete(schedule_id)
    return jsonify({"ok": True})


@api.get("/schedules/<schedule_id>/runs")
def schedule_runs(schedule_id: str):
    runs = _store("schedule_runs").query(where=[("schedule_id", "eq", schedule_id)],
                                         order_by="fired_at", order="desc", limit=50)
    return jsonify({"runs": runs})


@api.get("/cron/describe")
def cron_describe():
    expr = request.args.get("expr", "")
    return jsonify({"expr": expr, "description": _scheduler().describe_cron(expr)})


# ---------------------------------------------------------------------------
# 通知与集成
# ---------------------------------------------------------------------------

@api.get("/projects/<project_id>/integrations")
def list_integrations(project_id: str):
    return jsonify({"integrations": _notify().list(project_id)})


@api.post("/projects/<project_id>/integrations")
def create_integration(project_id: str):
    data = _payload()
    return jsonify(_notify().create(project_id, data))


@api.put("/integrations/<integration_id>")
def update_integration(integration_id: str):
    integration = _notify().get(integration_id)
    if integration is None:
        return _err("集成不存在", 404)
    data = _payload()
    patch = {k: data[k] for k in ("name", "type", "enabled", "config", "events")
             if k in data}
    return jsonify(_notify().update(integration_id, patch))


@api.delete("/integrations/<integration_id>")
def delete_integration(integration_id: str):
    _notify().delete(integration_id)
    return jsonify({"ok": True})


@api.post("/integrations/<integration_id>/test")
def test_integration(integration_id: str):
    result = _notify().send_test(integration_id)
    if "error" in result:
        return _err(result["error"], 404)
    return jsonify(result)


@api.get("/projects/<project_id>/events")
def list_events(project_id: str):
    return jsonify({"events": _notify().events(project_id)})


# ---------------------------------------------------------------------------
# 演示数据
# ---------------------------------------------------------------------------

@api.post("/seed/demo")
def seed_demo():
    """一键生成演示项目（含用例 / 套件 / 环境 / 计划 / 集成）。"""
    from .seed import seed_demo_data
    return jsonify(seed_demo_data(_registry(), _env_mgr(), _notify()))
