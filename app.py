"""自动化测试与持续集成平台 —— Flask 入口。

启动方式::

    python app.py             # 默认 http://127.0.0.1:8000
    python app.py --port 9000
"""

from __future__ import annotations

import argparse
import os
import sys

from flask import Flask, redirect, send_from_directory

# 保证以本目录为基准导入包
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from engine import (Scheduler, TestExecutor, EnvironmentManager,          # noqa: E402
                    CoverageAnalyzer, ReportGenerator, DefectManager,
                    NotificationManager)
from storage import StoreRegistry, BuildStoreRegistry                       # noqa: E402
from web import api                                                         # noqa: E402
from web.seed import seed_demo_data                                         # noqa: E402


def create_app(data_root: str | None = None) -> Flask:
    app = Flask(__name__, static_folder="static", static_url_path="/static")

    if data_root is None:
        data_root = os.path.join(BASE_DIR, "data")
    os.makedirs(data_root, exist_ok=True)

    # -- 存储层 -----------------------------------------------------------
    registry = StoreRegistry(os.path.join(data_root, "store"), shard_size=200)
    build_registry = BuildStoreRegistry(os.path.join(data_root, "builds"))

    # -- 引擎层 -----------------------------------------------------------
    executor = TestExecutor()
    env_manager = EnvironmentManager(registry, data_root)
    coverage = CoverageAnalyzer(build_registry)
    report_gen = ReportGenerator(build_registry)
    defects = DefectManager(registry)
    notify = NotificationManager(registry)
    scheduler = Scheduler(
        registry, build_registry, executor, env_manager,
        report_gen, coverage, defects, notify,
        max_build_workers=4, max_case_workers=8, tick_seconds=20,
    )

    # -- 注入 Flask config -------------------------------------------------
    app.config["DATA_ROOT"] = data_root
    app.config["STORE_REGISTRY"] = registry
    app.config["BUILD_REGISTRY"] = build_registry
    app.config["SCHEDULER"] = scheduler
    app.config["ENV_MANAGER"] = env_manager
    app.config["COVERAGE"] = coverage
    app.config["REPORT_GEN"] = report_gen
    app.config["DEFECTS"] = defects
    app.config["NOTIFY"] = notify
    app.config["JSON_AS_ASCII"] = False

    app.register_blueprint(api)

    # -- 页面路由 ---------------------------------------------------------
    @app.get("/")
    def index():
        return redirect("/page/projects")

    @app.get("/page/<path:name>")
    def page(name: str):
        if not name.endswith(".html"):
            name = name + ".html"
        return send_from_directory(os.path.join(BASE_DIR, "static", "pages"), name)

    # -- 首次启动：无数据则自动生成演示数据并触发一次构建 --------------------
    # 让各页面一打开就有内容可点、可测，报告/覆盖率/缺陷/监控也有初始数据。
    if not registry.store("projects").all():
        seeded = seed_demo_data(registry, env_manager, notify)
        try:
            scheduler.submit_build(
                seeded["project"]["id"], seeded["suite_id"], trigger="auto_seed")
        except Exception:  # noqa: BLE001
            pass

    # -- 调度器生命周期 ----------------------------------------------------
    scheduler.start()

    @app.teardown_appcontext
    def _teardown(_exc=None):
        pass

    return app


def main():
    parser = argparse.ArgumentParser(description="自动化测试与持续集成平台")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data", default=None, help="数据存储目录")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    app = create_app(args.data)
    print(f"* 自动化测试与持续集成平台已启动: http://{args.host}:{args.port}")
    print(f"* 数据目录: {app.config['DATA_ROOT']}")
    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == "__main__":
    main()
