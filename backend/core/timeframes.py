"""Resolve structured time periods from the interpreter using a source's reference date.

The model describes the period (calendar, relative, trailing or explicit range);
calendar arithmetic happens here so dates are exact and reproducible.
"""
from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Any

from .llm import SemanticClarification

UNITS = ("day", "week", "month", "quarter", "year")


class TimeframeError(ValueError):
    """The time object is malformed; the model can be asked to correct it."""


def _integer(value: Any, low: int, high: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise TimeframeError(f"{name} must be an integer from {low} to {high}")
    return value


def _iso(value: Any, name: str) -> str | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value).isoformat() if isinstance(value, str) and len(value) == 10 else _raise(name)
    except ValueError as exc:
        raise TimeframeError(f"{name} must be a valid YYYY-MM-DD date") from exc


def _raise(name: str) -> str:
    raise TimeframeError(f"{name} must be a valid YYYY-MM-DD date")


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _add_months(value: date, months: int) -> date:
    year, month = divmod(value.month - 1 + months, 12)
    year += value.year
    return date(year, month + 1, min(value.day, calendar.monthrange(year, month + 1)[1]))


def _period_start(anchor: date, unit: str) -> date:
    if unit == "day":
        return anchor
    if unit == "week":
        return anchor - timedelta(days=anchor.weekday())
    if unit == "month":
        return anchor.replace(day=1)
    if unit == "quarter":
        return date(anchor.year, 3 * ((anchor.month - 1) // 3) + 1, 1)
    return date(anchor.year, 1, 1)


def _shift(start: date, unit: str, count: int) -> date:
    if unit == "day":
        return start + timedelta(days=count)
    if unit == "week":
        return start + timedelta(days=7 * count)
    return _add_months(start, count * {"month": 1, "quarter": 3, "year": 12}[unit])


def _anchor(as_of: date | str | None) -> date:
    if as_of is None:
        raise SemanticClarification("This source has no approved reference date for relative periods. Use explicit dates such as 2025-01-01 to 2025-03-31.")
    return date.fromisoformat(as_of) if isinstance(as_of, str) else as_of


def resolve_timeframe(spec: Any, as_of: date | str | None) -> tuple[str | None, str | None]:
    if not isinstance(spec, dict):
        raise TimeframeError("time must be an object")
    kind = spec.get("type")
    if kind == "calendar":
        year = _integer(spec.get("year"), 1900, 2200, "year")
        quarter, month = spec.get("quarter"), spec.get("month")
        if quarter is not None and month is not None:
            raise TimeframeError("use quarter or month, not both")
        if quarter is not None:
            quarter = _integer(quarter, 1, 4, "quarter")
            start, end = date(year, 3 * quarter - 2, 1), _month_end(year, 3 * quarter)
        elif month is not None:
            month = _integer(month, 1, 12, "month")
            start, end = date(year, month, 1), _month_end(year, month)
        else:
            start, end = date(year, 1, 1), date(year, 12, 31)
    elif kind == "relative":
        anchor = _anchor(as_of)
        unit = spec.get("unit")
        if unit not in UNITS:
            raise TimeframeError("unit must be day, week, month, quarter or year")
        offset = _integer(spec.get("offset"), -240, 24, "offset")
        start = _shift(_period_start(anchor, unit), unit, offset)
        end = anchor if offset == 0 else _shift(start, unit, 1) - timedelta(days=1)
    elif kind == "trailing":
        anchor = _anchor(as_of)
        unit = spec.get("unit")
        if unit not in UNITS:
            raise TimeframeError("unit must be day, week, month, quarter or year")
        count = _integer(spec.get("count"), 1, 3660, "count")
        if unit in ("day", "week"):
            start = anchor - timedelta(days=count * (7 if unit == "week" else 1) - 1)
        else:
            start = _add_months(anchor, -count * {"month": 1, "quarter": 3, "year": 12}[unit]) + timedelta(days=1)
        end = anchor
    elif kind == "range":
        start_text, end_text = _iso(spec.get("start"), "start"), _iso(spec.get("end"), "end")
        if start_text is None and end_text is None:
            raise TimeframeError("a range needs a start or an end date")
        if start_text and end_text and start_text > end_text:
            raise TimeframeError("range start must not be after its end")
        start = date.fromisoformat(start_text) if start_text else None
        end = date.fromisoformat(end_text) if end_text else None
    else:
        raise TimeframeError("time type must be calendar, relative, trailing or range")
    if as_of is not None and start is not None and start > _anchor(as_of):
        raise SemanticClarification("That period starts after the data's reference date. Forecasts and future periods are not supported.", reason="unsupported")
    return (start.isoformat() if start else None), (end.isoformat() if end else None)
