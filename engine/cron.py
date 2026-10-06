"""5 段 cron 表达式解析与匹配。

支持标准 cron 五段式 ``分 时 日 月 周``：

- ``*``          任意值
- ``*/n``        每隔 n
- ``a-b``        区间
- ``a,b,c``      枚举
- ``n``          单个值

匹配基于本地时间，用于定时任务的到点触发判定。调度循环每隔
``TICK`` 秒调用一次 :func:`cron_matches`，判断某个计划此刻是否应当触发。
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import List


_WEEKDAYS = {"sun": 0, "mon": 1, "tue": 2, "wed": 3,
             "thu": 4, "fri": 5, "sat": 6}
_MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
           "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


@dataclass
class CronSchedule:
    """解析后的 cron 计划。"""

    minute: List[int] = field(default_factory=list)
    hour: List[int] = field(default_factory=list)
    day: List[int] = field(default_factory=list)
    month: List[int] = field(default_factory=list)
    weekday: List[int] = field(default_factory=list)
    raw: str = ""

    def matches(self, dt: datetime.datetime) -> bool:
        """判断给定时刻是否命中该计划。"""
        # cron 的星期采用 0=周日、1=周一…6=周六；Python 的 weekday() 是
        # 0=周一…6=周日，需 +1 取模对齐到 cron 约定。
        cron_weekday = (dt.weekday() + 1) % 7
        return (
            dt.minute in self.minute
            and dt.hour in self.hour
            and dt.day in self.day
            and dt.month in self.month
            and cron_weekday in self.weekday
        )


def _parse_field(field: str, lo: int, hi: int,
                 names: dict = None) -> List[int]:
    """解析一个 cron 字段为取值集合。"""
    values = set()
    field = field.strip().lower()
    if names and field in names:
        values.add(names[field])
        return sorted(values)

    for part in field.split(","):
        part = part.strip()
        if not part:
            continue
        if part == "*":
            values.update(range(lo, hi + 1))
        elif part.startswith("*/"):
            step = int(part[2:])
            values.update(range(lo, hi + 1, step))
        elif "-" in part:
            a, b = part.split("-", 1)
            if names and a in names:
                a = names[a]
            if names and b in names:
                b = names[b]
            values.update(range(int(a), int(b) + 1))
        else:
            if names and part in names:
                values.add(names[part])
            else:
                values.add(int(part))
    return sorted(values)


def parse_cron(expr: str) -> CronSchedule:
    """解析五段 cron 表达式。"""
    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(f"cron 表达式需要 5 个字段，收到 {len(parts)}: {expr!r}")
    minute, hour, day, month, weekday = parts
    return CronSchedule(
        minute=_parse_field(minute, 0, 59),
        hour=_parse_field(hour, 0, 23),
        day=_parse_field(day, 1, 31),
        month=_parse_field(month, 1, 12, _MONTHS),
        weekday=_parse_field(weekday, 0, 6, _WEEKDAYS),
        raw=expr,
    )


def cron_matches(expr: str, dt: datetime.datetime = None) -> bool:
    """判断 cron 表达式在给定时刻（默认当前时刻）是否命中。"""
    dt = dt or datetime.datetime.now()
    return parse_cron(expr).matches(dt)
