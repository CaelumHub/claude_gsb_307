"""存储层：文件锁 + 原子写 + 分片 JSON 存储 + 按项目/构建分片的构建结果存储。

本层是平台「JSON 文件在高并发写入下的原子性与一致性」这一难点的核心实现：

- :mod:`storage.lock`      跨进程/跨线程文件锁（flock）
- :mod:`storage.atomic`    临时文件 + ``os.replace`` 的原子替换写入
- :mod:`storage.sharded`   通用分片 JSON 存储（项目 / 用例 / 套件 / 缺陷 / 环境 / 计划 / 集成）
- :mod:`storage.buildstore` 按「项目 + 构建」二次分片的执行结果存储，
                          负责多 worker 并发写结果时的收集与聚合
"""

from .lock import FileLock, LockTimeout, lock_path_for
from .atomic import atomic_write_json, read_json
from .sharded import ShardedStore, StoreRegistry
from .buildstore import BuildStore, BuildStoreRegistry

__all__ = [
    "FileLock",
    "LockTimeout",
    "lock_path_for",
    "atomic_write_json",
    "read_json",
    "ShardedStore",
    "StoreRegistry",
    "BuildStore",
    "BuildStoreRegistry",
]
