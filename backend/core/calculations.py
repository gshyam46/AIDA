"""Deterministic calculations over aggregated result rows.

The interpreter may request only these operations over approved measures. Code
does the arithmetic after read-only SQL has returned at most 100 aggregate rows.
"""
from __future__ import annotations

import copy
import math
import re
from typing import Any, Callable

OPERATIONS = {"ratio": 2, "difference": 2, "share_of_total": 1, "running_total": 1, "percent_change": 1}
COMPARISONS = {"eq", "gt", "gte", "lt", "lte"}
_ID = re.compile(r"calculation_[1-3]\Z")


class CalculationError(ValueError):
    """A saved or interpreted calculation does not fit the plan it belongs to."""


def plan_shape(plan: dict[str, Any]) -> tuple[list[str], list[str]]:
    if plan.get("version") == 2:
        return list(plan.get("metrics") or []), list(plan.get("dimensions") or [])
    return [plan.get("metric")], [plan["dimension"]] if plan.get("dimension") else []


def validate(specs: Any, post: Any, plan: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    metrics, groups = plan_shape(plan)
    specs = [] if specs is None else specs
    if not isinstance(specs, list) or len(specs) > 3:
        raise CalculationError("Use at most three calculations.")
    clean, identifiers = [], set()
    for spec in specs:
        if not isinstance(spec, dict) or set(spec) - {"id", "label", "op", "inputs", "format"}:
            raise CalculationError("Calculations need an id, label, operation and inputs.")
        identifier, op, inputs, label = spec.get("id"), spec.get("op"), spec.get("inputs"), spec.get("label")
        if not isinstance(identifier, str) or not _ID.fullmatch(identifier) or identifier in identifiers:
            raise CalculationError("Calculation ids must be calculation_1 to calculation_3.")
        if op not in OPERATIONS or not isinstance(inputs, list) or len(inputs) != OPERATIONS[op] or any(item not in metrics for item in inputs):
            raise CalculationError("A calculation must use a supported operation over selected measures.")
        if op in ("ratio", "difference") and inputs[0] == inputs[1]:
            raise CalculationError("A ratio or difference needs two different measures.")
        if op == "share_of_total" and not groups:
            raise CalculationError("A share of total needs a grouping.")
        if op in ("running_total", "percent_change") and groups != ["month"]:
            raise CalculationError("Running totals and percent change need a monthly grouping.")
        if not isinstance(label, str) or not 1 <= len(label.strip()) <= 80 or any(ord(char) < 32 for char in label):
            raise CalculationError("Give each calculation a short label.")
        identifiers.add(identifier)
        clean.append({"id": identifier, "label": label.strip(), "op": op, "inputs": list(inputs)})
    post = {} if post is None else post
    if not isinstance(post, dict) or set(post) - {"thresholds", "sort", "limit"}:
        raise CalculationError("Calculation ordering supports thresholds, sort and limit only.")
    thresholds = post.get("thresholds") or []
    if not isinstance(thresholds, list) or len(thresholds) > 3:
        raise CalculationError("Use at most three calculation thresholds.")
    for item in thresholds:
        if (not isinstance(item, dict) or set(item) != {"target", "op", "value"} or item["target"] not in identifiers
                or item["op"] not in COMPARISONS or isinstance(item["value"], bool) or not isinstance(item["value"], (int, float))
                or not math.isfinite(item["value"])):
            raise CalculationError("Calculation thresholds compare a calculation with a number.")
    if thresholds and not groups:
        raise CalculationError("A calculation threshold needs a grouping.")
    sort = post.get("sort")
    if sort is not None and (not isinstance(sort, dict) or set(sort) != {"by", "direction"} or sort["by"] not in identifiers or sort["direction"] not in ("asc", "desc")):
        raise CalculationError("Sort by a calculation in ascending or descending order.")
    limit = post.get("limit")
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100):
        raise CalculationError("A calculation limit must be from 1 to 100.")
    clean_post = {key: value for key, value in {"thresholds": thresholds, "sort": sort, "limit": limit}.items() if value}
    return clean, copy.deepcopy(clean_post)


def output_format(spec: dict[str, Any], formats: dict[str, str]) -> str:
    if spec["op"] in ("share_of_total", "percent_change"):
        return "percent"
    if spec["op"] == "ratio":
        return "number"
    return formats.get(spec["inputs"][0], "number")


def _number(value: Any) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None


def _finite(value: float | None) -> float | None:
    return round(value, 6) if value is not None and math.isfinite(value) else None


def _divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return _finite(numerator / denominator)


def _passes(value: float | None, op: str, bound: float) -> bool:
    if value is None:
        return False
    return {"eq": math.isclose(value, bound, rel_tol=1e-9, abs_tol=1e-9), "gt": value > bound, "gte": value >= bound,
            "lt": value < bound, "lte": value <= bound}[op]


def apply(rows: list[dict[str, Any]], specs: list[dict[str, Any]], post: dict[str, Any], column_for: Callable[[str], str],
          total: dict[str, Any], group: str | None) -> list[dict[str, Any]]:
    output = [dict(row) for row in rows]
    for spec in specs:
        first = column_for(spec["inputs"][0])
        second = column_for(spec["inputs"][1]) if len(spec["inputs"]) > 1 else None
        if spec["op"] == "ratio":
            for row in output:
                row[spec["id"]] = _divide(_number(row.get(first)), _number(row.get(second)))
        elif spec["op"] == "difference":
            for row in output:
                left, right = _number(row.get(first)), _number(row.get(second))
                row[spec["id"]] = _finite(left - right) if left is not None and right is not None else None
        elif spec["op"] == "share_of_total":
            denominator = _number(total.get(first))
            for row in output:
                row[spec["id"]] = _divide(_number(row.get(first)), denominator)
        else:
            ordered = sorted(range(len(output)), key=lambda index: (output[index].get(group) is None, str(output[index].get(group) or "")))
            running, previous, seen = 0.0, None, False
            for index in ordered:
                current = _number(output[index].get(first))
                if spec["op"] == "running_total":
                    if current is not None:
                        running += current
                        seen = True
                    output[index][spec["id"]] = _finite(running) if seen else None
                else:
                    output[index][spec["id"]] = _divide(current - previous, abs(previous)) if current is not None and previous is not None else None
                    previous = current
    for item in post.get("thresholds", []):
        output = [row for row in output if _passes(row.get(item["target"]), item["op"], float(item["value"]))]
    sort = post.get("sort")
    if sort:
        present = [row for row in output if row.get(sort["by"]) is not None]
        missing = [row for row in output if row.get(sort["by"]) is None]
        present.sort(key=lambda row: row[sort["by"]], reverse=sort["direction"] == "desc")
        output = present + missing
    if post.get("limit"):
        output = output[:post["limit"]]
    return output
