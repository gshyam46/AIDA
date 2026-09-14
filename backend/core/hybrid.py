"""Model interpretation is separate from deterministic, source-scoped execution."""
from __future__ import annotations

import time
from typing import Any

from . import calculations
from .interpreter import Interpreter
from .llm import ModelUnavailable, SemanticClarification


class HybridAnalytics:
    def __init__(self, registry, interpreter: Interpreter | None = None):
        self.registry = registry
        self.interpreter = interpreter or Interpreter()

    def catalog(self, source_id: str = "commerce", principal: dict[str, Any] | None = None) -> dict[str, Any]:
        engine = self.registry.engine(source_id, principal)
        catalog = engine.catalog()
        catalog["dataset"]["snapshot_updated_at"] = getattr(engine, "snapshot_updated_at", None)
        return catalog

    def query(self, question: str | None = None, plan: dict | None = None, source_id: str = "commerce",
              catalog_version: str | None = None, principal: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.perf_counter()
        telemetry: dict[str, Any] = {"model_calls": 0, "guard_calls": 0, "interpretation_source": "visual_builder", "interpretation_cache_hit": False,
                                     "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "estimated_model_cost_usd": 0}
        catalog: dict[str, Any] = {}
        try:
            engine = self.registry.engine(source_id, principal)
            catalog = engine.catalog()
            if catalog_version is not None and catalog_version != engine.catalog_version:
                return self._failure("The source definitions changed. Review the current catalog and save this analysis again.", "catalog_changed", telemetry, started)
            if (question is None) == (plan is None):
                return self._failure("Provide either a question or a query plan.", "invalid_request", telemetry, started)
            interpretation = None
            if question is not None:
                interpretation = self.interpreter.interpret(question, catalog)
                telemetry.update(interpretation.telemetry)
                base, specs, post = interpretation.plan, interpretation.calculations, interpretation.post
            elif isinstance(plan, dict):
                base = {key: value for key, value in plan.items() if key not in ("calculations", "post")}
                specs, post = calculations.validate(plan.get("calculations"), plan.get("post"), base)
            else:
                base, specs, post = plan, [], {}
            result = self._execute(engine, base, specs, post)
            meta = dict(result.get("meta", {}))
            database_ms = meta.get("elapsed_ms", 0)
            meta.update(telemetry)
            meta.update({"engine": "hybrid", "source_id": source_id, "database_time_ms": database_ms,
                         "elapsed_ms": round((time.perf_counter() - started) * 1000, 2)})
            meta["execution_time_ms"] = meta["elapsed_ms"]
            meta["snapshot_updated_at"] = getattr(engine, "snapshot_updated_at", None)
            result["meta"] = result["metadata"] = meta
            result["source_id"] = source_id
            if interpretation is not None:
                result["semantic_ir"] = interpretation.ir
                result["interpretation"] = {"notes": interpretation.notes, "mentions": interpretation.mentions}
            return result
        except (SemanticClarification, ModelUnavailable) as exc:
            telemetry.update(getattr(exc, "telemetry", None) or {})
            suggestions = getattr(exc, "options", None) or list(catalog.get("examples", []))[:4]
            error_type = "model_unavailable" if isinstance(exc, ModelUnavailable) else "clarification_required"
            return self._failure(str(exc), error_type, telemetry, started, suggestions, getattr(exc, "reason", None))
        except calculations.CalculationError as exc:
            return self._failure(str(exc), "clarification_required", telemetry, started, list(catalog.get("examples", []))[:4])

    @staticmethod
    def _execute(engine, plan: Any, specs: list[dict[str, Any]], post: dict[str, Any]) -> dict[str, Any]:
        result = engine.query(plan=plan)
        if not specs or not result.get("success"):
            return result
        canonical = result["plan"]
        relational = canonical.get("version") == 2
        group = (canonical["dimensions"][0] if canonical["dimensions"] else None) if relational else canonical.get("dimension")
        total: dict[str, Any] = {}
        if any(spec["op"] == "share_of_total" for spec in specs):
            if relational:
                total_plan = {**canonical, "dimensions": [], "having": [], "comparison": None, "sort": {"field": canonical["metrics"][0], "direction": "desc"}, "limit": 1}
            else:
                total_plan = {**canonical, "dimension": None, "sort": "value_desc", "limit": 1}
            totals = engine.query(plan=total_plan)
            if not totals.get("success"):
                return totals
            total = totals["data"][0] if totals["data"] else {}
        column = (lambda metric: metric) if relational else (lambda metric: "value")
        rows = calculations.apply(result["data"], specs, post, column, total, group)
        chart = result.setdefault("chart", {})
        formats = dict(chart.get("formats") or {}) if relational else {canonical["metric"]: chart.get("format", "number")}
        described = [{**spec, "format": calculations.output_format(spec, formats)} for spec in specs]
        result["data"] = result["results"] = rows
        result["columns"] = [*result.get("columns", []), *(spec["id"] for spec in described)]
        result["calculations"] = described
        chart["calculations"] = [spec["id"] for spec in described]
        chart["formats"] = {**chart.get("formats", {}), **{spec["id"]: spec["format"] for spec in described}}
        if isinstance(result.get("lineage"), dict):
            result["lineage"]["operations"] = [*result["lineage"].get("operations", []), "calculation"]
        result["explanation"] = ((result.get("explanation") or "") + " Calculated in code after aggregation: "
                                 + "; ".join(f"{spec['label']} ({spec['op'].replace('_', ' ')})" for spec in described) + ".").strip()
        result["plan"] = {**canonical, "calculations": specs, **({"post": post} if post else {})}
        result.setdefault("meta", {})["row_count"] = len(rows)
        return result

    @staticmethod
    def _failure(message: str, error_type: str, telemetry: dict, started: float,
                 suggestions: list[str] | None = None, reason: str | None = None) -> dict:
        meta = {**telemetry, "engine": "hybrid", "elapsed_ms": round((time.perf_counter() - started) * 1000, 2)}
        payload = {"success": False, "data": [], "results": [], "error": message, "error_type": error_type, "meta": meta, "metadata": meta}
        if suggestions:
            payload["suggestions"] = list(suggestions)[:4]
        if reason:
            payload["clarification_reason"] = reason
        return payload
