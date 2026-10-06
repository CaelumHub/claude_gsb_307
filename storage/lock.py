"""跨进程 / 跨线程文件锁。

基于 ``fcntl.flock`` 实现，用于保护 JSON 分片存储的并发读写。

为什么需要它
------------
JSON 文件不像数据库有事务。当多个测试 worker 同时把各自的结果写回同一个
分片文件时，常见的做法是「读文件 → 改内存 → 写回文件」，这中间没有临界区，
两个 worker 可能读到同一份旧数据、各自改一份、先后写回，后写的把先写的
覆盖掉——于是丢结果、计数错乱。

本模块用「锁文件 + flock」把每个分片 / 元数据的读-改-写变成一个临界区：

- 写操作使用排他锁 ``LOCK_EX``，读操作使用共享锁 ``LOCK_SH``；
- 带超时的轮询等待，避免死等；
- 配合 :func:`storage.atomic.atomic_write_json` 的「临时文件 + os.replace
  原子替换」，保证即便在写的过程中进程崩溃，磁盘上也永远不会留下半截
  JSON 文件。

锁文件独立于数据文件（``<数据文件>.lock``），这样锁文件里永远不承载业务
数据，也不会因为数据文件被替换（os.replace）而导致锁失效。
"""

from __future__ import annotations

import fcntl
import os
import time
from typing import Optional


class LockTimeout(RuntimeError):
    """等待文件锁超时。"""

    def __init__(self, path: str, timeout: float):
        self.path = path
        self.timeout = timeout
        super().__init__(f"无法在 {timeout:.1f}s 内获取文件锁: {path}")


class FileLock:
    """基于 flock 的文件锁上下文管理器。

    用法::

        with FileLock("/data/projects/meta.json.lock"):          # 排他锁
            ...   # 读-改-写临界区

        with FileLock("/data/projects/meta.json.lock", mode="shared"):
            ...   # 读临界区

    ``mode`` 为 ``"exclusive"``（默认）或 ``"shared"``。
    """

    def __init__(self, path: str, timeout: float = 30.0,
                 mode: str = "exclusive"):
        self.path = path
        self.timeout = timeout
        self.mode = mode
        self._fd: Optional[object] = None

    # -- 生命周期 ---------------------------------------------------------
    def acquire(self) -> "FileLock":
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        # 以追加模式打开：锁文件本身不会被其它进程截断
        self._fd = open(self.path, "a")
        operation = fcntl.LOCK_EX if self.mode == "exclusive" else fcntl.LOCK_SH
        deadline = time.monotonic() + self.timeout

        while True:
            try:
                fcntl.flock(self._fd.fileno(), operation | fcntl.LOCK_NB)
                return self
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    self.release()
                    raise LockTimeout(self.path, self.timeout)
                time.sleep(0.02)

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                self._fd.close()
            except OSError:
                pass
            self._fd = None

    # -- 上下文协议 -------------------------------------------------------
    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


def lock_path_for(target: str) -> str:
    """给定数据文件路径，返回对应的锁文件路径。"""
    return target + ".lock"
