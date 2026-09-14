"""Model interpretation is separate from deterministic, source-scoped execution."""
from __future__ import annotations

import time
from typing import Any

from .semantic import ModelUnavailable, SemanticClarification, SemanticParser
from .relational_semantic import RelationalSemanticParser


class HybridAnalytics:
    def __init__(self, registry, parser: SemanticParser | None = None):
        self.registry = registry
        self.parser = parser or SemanticParser()
        self.relational_parser = RelationalSemanticParser(shared_parser=self.parser) if parser is None or isinstance(parser, SemanticParser) else parser

    def catalog(self, source_id: str = "commerce") -> dict[str, Any]:
        engine = self.registry.engine(source_id)
        catalog = engine.catalog()
        catalog["dataset"]["snapshot_updated_at"] = getattr(engine, "snapshot_updated_at", None)
        return catalog

    def query(self, question: str | None = None, plan: dict | None = None,
              source_id: str = "commerce", catalog_version: str | None = None) -> dict[str, Any]:
        started = time.perf_counter()
        telemetry = {"model_calls": 0, "interpretation_source": "visual_builder",
                     "interpretation_cache_hit": False, "prompt_tokens": 0,
                     "completion_tokens": 0, "total_tokens": 0,
                     "external_model_calls": 0, "model_api_cost_usd": 0}
        semantic_ir = None
        try:
            engine = self.registry.engine(source_id)
            catalog = engine.catalog()
            if catalog_version is not None and catalog_version != engine.catalog_version:
                return self._failure("The source definitions changed. Review the current catalog and save this analysis again.", "catalog_changed", telemetry, started)
            if (question is None) == (plan is None):
                return self._failure("Provide either a question or a query plan.", "invalid_request", telemetry, started)
            if question is not None:
                interpreter = self.relational_parser if catalog.get("capabilities", {}).get("relational") else self.parser
                interpreted = interpreter.parse(question, catalog, as_of=catalog["dataset"]["as_of"])
                plan = interpreted.plan
                semantic_ir = interpreted.ir
                telemetry.update(interpreted.telemetry)
            result = engine.query(plan=plan)
            meta = dict(result.get("meta", result.get("metadata", {})))
            database_ms = meta.get("elapsed_ms", meta.get("execution_time_ms", 0))
            meta.update(telemetry)
            meta.update({"engine": "hybrid", "source_id": source_id,
                         "database_time_ms": database_ms,
                         "elapsed_ms": round((time.perf_counter() - started) * 1000, 2)})
            meta["execution_time_ms"] = meta["elapsed_ms"]
            meta["snapshot_updated_at"] = getattr(engine, "snapshot_updated_at", None)
            result["meta"] = result["metadata"] = meta
            result["source_id"] = source_id
            if semantic_ir is not None:
                result["semantic_ir"] = semantic_ir
            return result
        except (SemanticClarification, ModelUnavailable) as exc:
            telemetry.update(getattr(exc, "telemetry", {}))
            return self._failure(str(exc), "model_unavailable" if isinstance(exc, ModelUnavailable) else "clarification_required", telemetry, started)

    @staticmethod
    def _failure(message: str, error_type: str, telemetry: dict, started: float) -> dict:
        meta = {**telemetry, "engine": "hybrid", "elapsed_ms": round((time.perf_counter() - started) * 1000, 2)}
        return {"success": False, "data": [], "results": [], "error": message,
                "error_type": error_type, "meta": meta, "metadata": meta}
