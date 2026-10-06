"""原子 JSON 读写原语。

要点
----
直接 ``json.dump(obj, open(path, "w"))`` 是危险写法：写的过程可能被另一个
进程读到一半，或进程崩溃后留下截断的文件。这里统一走：

    写临时文件 -> fsync -> os.replace(tmp, path)

``os.replace`` 在同一文件系统上是原子的 rename，读方要么读到旧文件、要么
读到新文件，绝不会读到中间态。这是「JSON 文件原子性」的最底层保证。
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from typing import Any


def atomic_write_json(path: str, obj: Any, indent: int = 2) -> None:
    """原子写入 JSON：先写临时文件并落盘，再 ``os.replace`` 覆盖目标。

    临时文件与目标放在同一目录下（保证同一文件系统，rename 才是原子的），
    文件名带 pid + 随机后缀，避免并发写临时文件互相踩。
    """
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    fd, tmp = tempfile.mkstemp(
        prefix=f".{os.path.basename(path)}.",
        suffix=f".{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp",
        dir=directory or ".",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=indent)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        # 失败时清理临时文件，不留下垃圾
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def read_json(path: str, default: Any) -> Any:
    """读取 JSON 文件；不存在或损坏时返回 ``default``。"""
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        # 读到损坏文件时按缺失处理，避免把整个请求打挂；
        # 正常情况下原子的 os.replace 不会产生损坏文件。
        return default
