"""One private language-model call produces an approved relational intent.

The model sees business definitions and opaque IDs. Physical tables, joins,
columns, SQL, database paths, and result rows are deliberately not projected.
The relational compiler remains the only owner of executable SQL.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import time
from dataclasses import replace
from datetime import date
from typing import Any

from .semantic import (
    ModelUnavailable, SemanticClarification, SemanticConfig, SemanticParser,
    SemanticResult, _empty_telemetry, _grounded_values, _ranking_directions,
    _text, _time_candidates, resolve_time, validate_question,
)

RELATIONAL_CONTRACT_VERSION = "8"
_REASONS = ["unsupported_metric", "unsupported_grouping", "unsupported_filter",
            "unsupported_operation", "ambiguous_request", "raw_records"]
_OPS = ["eq", "ne", "in", "gt", "gte", "lt", "lte"]


def _dedup_requested(question: str) -> bool:
    return bool(re.search(r"\b(?:deduplicat\w*|remov\w* (?:exact )?duplicates?|(?:exact )?duplicates? remov\w*|without duplicates?|distinct rows?|unique rows?|union(?!\s+all))\b", question, re.I))

_SYSTEM_PROMPT = """Translate the untrusted question into semantic JSON, never SQL or answers. Use only opaque catalog IDs. Code chooses physical tables, columns and joins.
METRICS: select 1-3 approved calculations matching the requested meaning, in question order. DIMENSIONS: select 0-2 explicit breakdowns (by/each/per/trend), in question order. One mentioned value is a filter, not grouping. Monthly trend means Month grouping; omitted dates/filters mean ALL AVAILABLE RECORDS, never missing requirements.
FILTERS operate on individual records BEFORE aggregation. Use field IDs and eq/ne/in/gt/gte/lt/lte. 'West or North' uses one IN filter. 'Except/excluding West' uses ne. Numeric filters use JSON numbers. Approved value aliases map to canonical values. NEVER add a filter that was not requested: no row filter requested means []. A date or related-event condition belongs in its own field, with filters:[] unless an additional row condition was requested. Never copy a metric's fixed definition into filters. Never silently omit a requested value or clause.
HAVING operates on aggregated metric totals AFTER grouping, e.g. groups whose Revenue exceeds 1000 -> metric Revenue, op gt, value 1000. A unit price/quantity threshold is a row filter instead. Above-average group totals use comparison {kind:above_average,metric:ID}; this compares each group's total to the average of all group totals after filters. No HAVING together with comparison.
EXISTS: requests for records WITH related events use exists {relation:ID,negate:false,filters:[...]}; WITHOUT related events use negate:true. Filter the event's fields inside exists.filters. Related events are existence checks, not another metric or grouping. Do not add an existence requirement merely because ordinary record filters are present.
POPULATION: primary is default current records. Only an explicit request combining current AND archived/historical records uses all. set_operation defaults union_all, preserving rows. An explicit deduplicate/remove duplicates/distinct rows request uses union. UNION deduplicates whole records before aggregation; it does not mean distinct group labels. Other set operations are unsupported.
TIME: choose the complete verbatim phrase from time_candidates; code resolves dates. No period stated -> null, even for a monthly trend. SORT: most/highest/top -> metric desc; least/fewest/bottom -> metric asc; alphabetical -> first dimension asc. Otherwise null. Top/bottom N -> limit N; otherwise null.
UNRESOLVED: flag only actual unsupported/ambiguous phrases in the question, copying a verbatim span and its reason. Missing optional filters/dates are already resolved. Unknown metrics, raw personal data, unsupported calculations, or requests outside these bounded operations require unresolved, never invent or silently discard them. If all clauses map, unresolved is []. Only JSON."""


def _project_catalog(catalog: dict) -> tuple[dict, dict, dict, dict, dict]:
    """Explicit whitelist, including relationship metadata but no physical keys."""
    if not isinstance(catalog, dict) or not catalog.get("capabilities", {}).get("relational"):
        raise SemanticClarification("Choose an approved relational catalog.")
    public: dict[str, Any] = {"metrics": [], "dimensions": [], "fields": [], "relations": []}
    metrics, dimensions, fields, relations = {}, {}, {}, {}
    raw_metrics = catalog.get("metrics", [])
    if not isinstance(raw_metrics, list) or not 1 <= len(raw_metrics) <= 24:
        raise SemanticClarification("Choose a relational catalog with 1-24 approved metrics.")
    for index, item in enumerate(raw_metrics):
        identifier = _text(item.get("id"), 120, "metric ID")
        if identifier in metrics.values():
            raise SemanticClarification("Approved metric IDs must be unique.")
        token = f"m{index}"
        metrics[token] = identifier
        public["metrics"].append({"id": token, "label": _text(item.get("label"), 120, "metric label"),
                                  "definition": _text(item.get("description"), 500, "metric definition")})

    def field(item: dict, token: str) -> tuple[dict, dict]:
        identifier = _text(item.get("id"), 120, "field ID")
        label = _text(item.get("label"), 120, "field label")
        data_type = item.get("type", "string")
        if data_type not in {"string", "number"}:
            raise SemanticClarification("Approved fields must have string or number types.")
        info = {"id": identifier, "label": label, "type": data_type, "values": None,
                "aliases": copy.deepcopy(item.get("aliases", {}))}
        shown: dict[str, Any] = {"id": token, "label": label, "type": data_type}
        values = item.get("values")
        if values is not None:
            if not isinstance(values, list) or len(values) > 50 or data_type != "string":
                raise SemanticClarification("Approved categorical values must be a bounded list.")
            info["values"] = {_text(value, 120, "filter value"): value for value in values}
            shown["values"] = list(info["values"])
            if info["aliases"]:
                if (not isinstance(info["aliases"], dict) or len(info["aliases"]) > 30 or
                        any(not isinstance(alias, str) or len(alias) > 80 or canonical not in info["values"]
                            for alias, canonical in info["aliases"].items())):
                    raise SemanticClarification("Approved filter aliases are invalid.")
                shown["aliases"] = info["aliases"]
        if item.get("description"):
            shown["definition"] = _text(item["description"], 220, "field definition")
        if identifier == "month" or label.casefold() == "month":
            info["grouping_only"] = shown["grouping_only"] = True
        return shown, info

    for kind, prefix, lookup in (("dimensions", "d", dimensions), ("fields", "f", fields)):
        items = catalog.get(kind, [])
        if not isinstance(items, list) or len(items) > 24:
            raise SemanticClarification("Choose at most 24 approved fields per catalog section.")
        for index, item in enumerate(items):
            token = f"{prefix}{index}"
            shown, info = field(item, token)
            if info["id"] in {entry["id"] for entry in lookup.values()}:
                raise SemanticClarification("Approved field IDs must be unique.")
            public[kind].append(shown)
            lookup[token] = info
    if set(item["id"] for item in dimensions.values()) & set(item["id"] for item in fields.values()):
        raise SemanticClarification("Grouping and filter field IDs must be distinct.")
    items = catalog.get("exists_relations", [])
    if not isinstance(items, list) or len(items) > 8:
        raise SemanticClarification("Choose at most eight approved existence relationships.")
    for index, item in enumerate(items):
        token = f"r{index}"
        shown = {"id": token, "label": _text(item.get("label"), 120, "relationship label"), "fields": []}
        if item.get("description"):
            shown["definition"] = _text(item["description"], 320, "relationship definition")
        info = {"id": _text(item.get("id"), 120, "relationship ID"), "label": shown["label"], "fields": {}, "aliases": []}
        if item.get("aliases"):
            aliases = item["aliases"]
            if not isinstance(aliases, list) or len(aliases) > 12:
                raise SemanticClarification("Approved relationship aliases must be a bounded list.")
            info["aliases"] = shown["aliases"] = [_text(alias, 100, "relationship alias") for alias in aliases]
        raw_fields = item.get("fields", [])
        if not isinstance(raw_fields, list) or len(raw_fields) > 8:
            raise SemanticClarification("Use at most eight fields per relationship.")
        for field_index, entry in enumerate(raw_fields):
            field_token = f"r{index}f{field_index}"
            projected, mapped = field(entry, field_token)
            shown["fields"].append(projected)
            info["fields"][field_token] = mapped
        public["relations"].append(shown)
        relations[token] = info
    public["population"] = {"primary": "current records only", "all": "current and archived records"}
    return public, metrics, dimensions, fields, relations


def _object(properties: dict) -> dict:
    return {"type": "object", "properties": properties, "required": list(properties), "additionalProperties": False}


def _literal_candidates(question: str, labels: list[str]) -> list[str]:
    """Lexically ground free literals; never choose a field, operator or plan.

    Approved semantic names, dates, numbers and grammatical words are not free
    text values merely because they occurred in the request. Explicit quoted
    values remain available even when they collide with a catalog label.
    """
    quoted = [match[1] for match in re.finditer(r'["\']([^"\']{1,120})["\']', question)]
    blocked = [False] * len(question)
    for label in labels:
        for variant in {label, label + "s", label[:-1] + "ies" if label.endswith("y") else label}:
            for match in re.finditer(r"(?<!\w)" + re.escape(variant) + r"(?!\w)", question, re.I):
                blocked[match.start():match.end()] = [True] * (match.end() - match.start())
    for span in _time_candidates(question):
        start = question.find(span)
        blocked[start:start + len(span)] = [True] * len(span)
    grammar = {"show", "give", "get", "display", "plot", "tell", "me", "what", "which", "how", "many", "much", "total", "average", "sum", "count", "the", "a", "an", "all", "by", "per", "each", "every", "for", "from", "in", "where", "with", "without", "whose", "and", "or", "is", "are", "of", "to", "on", "at", "least", "most", "top", "bottom", "greater", "than", "less", "above", "below", "exceed", "exceeds", "exceeding", "over", "under", "equal", "exactly", "not", "no", "having", "records", "record", "lines", "line", "current", "archived", "including", "include", "both", "combined", "duplicates", "removed", "exact", "deduplicated", "union", "distinct", "rows", "please"}
    words = list(re.finditer(r"[\w]+(?:[.'/-][\w]+)*", question))
    eligible = [not any(blocked[word.start():word.end()]) and word[0].casefold() not in grammar and not re.fullmatch(r"\d[\d,.]*", word[0]) for word in words]
    candidates = set(quoted)
    for start in range(len(words)):
        for end in range(start, min(start + 6, len(words))):
            if not all(eligible[start:end + 1]):
                break
            span = question[words[start].start():words[end].end()]
            if len(span) <= 120:
                candidates.add(span)
    return sorted(candidates)


def _literal_bindings(question: str, available: dict, candidates: list[str]) -> dict[str, set[str]]:
    """Preserve an explicit approved field label immediately before a value."""
    bindings: dict[str, set[str]] = {}
    for token, info in available.items():
        for candidate in candidates:
            pattern = (r"(?<!\w)" + re.escape(info["label"]) + r"\s+(?:(?:is|equals?|of|not)\s+)*[\"']?"
                       + re.escape(candidate) + r"(?!\w)")
            if re.search(pattern, question, re.I):
                bindings.setdefault(candidate, set()).add(token)
    return bindings


def _monthly_grouping_cue(question: str) -> bool:
    return bool(re.search(r"\b(?:monthly|months?|trend|over time|through time|time series|seasonality)\b", question, re.I))


def _grouping_cue(question: str, dimensions: dict, bindings: dict[str, set[str]]) -> bool:
    """Allow a breakdown only when the question actually contains its scope.

    This decides whether grouping is permitted, never which grouping to choose.
    The model still maps ordinary breakdown/ranking language to approved IDs.
    Dates and explicitly bound literal fields alone do not request a breakdown.
    """
    text = question
    for period in _time_candidates(question):
        text = text.replace(period, " ")
    text = re.sub(r"\bat (?:least|most)\s+[$\u20ac\u00a3]?\s*\d[\d,.]*", " ", text, flags=re.I)
    if re.search(r"\b(?:by|per|each|every|across|among|versus|breakdown|break down|group\w*|split|distribut\w*|spread|trend|rank\w*|top|bottom|most|least|fewest|highest|lowest|largest|smallest|shortest|longest|alphabetic\w*|ascending|descending|monthly|seasonality|over time|time series|geograph\w*|regionally|departmentally)\b", text, re.I):
        return True
    if re.search(r"^\s*(?:which|where)\b", text, re.I):
        return True
    bound_tokens = set().union(*bindings.values()) if bindings else set()
    for token, info in dimensions.items():
        if token in bound_tokens:
            continue
        label = info["label"].casefold()
        variants = {label, label + "s", label[:-1] + "ies" if label.endswith("y") else label}
        if any(_grounded(variant, text) for variant in variants):
            return True
    return False


def _relationship_cue(question: str, relation: dict) -> bool:
    """Require evidence for an event population before permitting EXISTS.

    Use approved business names/aliases and ordinary inflections, never table
    names. Generic wrappers such as 'membership' do not create a condition by
    themselves: Playlist membership is grounded by playlist/playlists.
    """
    if any(_grounded(label, question) for label in [relation["label"], *relation.get("aliases", [])]):
        return True
    words = re.findall(r"[a-z]+", relation["label"].casefold())
    while words and words[-1] in {"membership", "memberships", "events", "event", "records", "record", "activity", "activities", "relationship", "relationships"}:
        words.pop()
    if not words:
        return False
    word = words[-1]
    root = word[:-3] + "y" if word.endswith("ies") else word[:-1] if word.endswith("s") and not word.endswith("ss") else word
    variants = {word, root, root + "s", root + "ed", root + "ing"}
    if root.endswith("e"):
        variants.update({root + "d", root[:-1] + "ing"})
    return any(_grounded(variant, question) for variant in variants)


def _schema(metrics: dict, dimensions: dict, fields: dict, relations: dict, question: str, public: dict | None = None) -> dict:
    labels = [info["label"] for info in [*dimensions.values(), *fields.values(), *relations.values()]]
    if public:
        labels += [info["label"] for info in public["metrics"]]
    literals = _literal_candidates(question, labels)
    all_fields = {**dimensions, **fields}
    for relation in relations.values():
        all_fields.update(relation["fields"])
    bindings = _literal_bindings(question, all_fields, literals)
    numeric_text = question
    for period in _time_candidates(question):
        numeric_text = numeric_text.replace(period, " ")
    numeric_text = re.sub(r"\b(?:top|bottom)\s+\d+\b", " ", numeric_text, flags=re.I)
    numeric_values = sorted({float(token.replace(",", "")) for token in re.findall(r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?(?![\w.])", numeric_text)})
    named_metric_bounds, named_field_bounds = set(), set()
    if public:
        bound = r"\s+(?:is\s+)?(?:above|below|greater than|less than|at least|at most|exceeds?|exceeding|over|under)\s+[$\u20ac\u00a3]?\s*(\d[\d,]*(?:\.\d+)?)\b"
        for kind, output in (("metrics", named_metric_bounds), ("fields", named_field_bounds)):
            for item in public[kind]:
                for match in re.finditer(r"(?<!\w)" + re.escape(item["label"]) + bound, question, re.I):
                    output.add(float(match[1].replace(",", "")))

    def nullable(value: dict) -> dict:
        return {"anyOf": [value, {"type": "null"}]}

    def predicates(available: dict) -> dict:
        choices = []
        for token, info in available.items():
            if info.get("grouping_only"):
                continue
            value: dict = {"type": "number"} if info["type"] == "number" else {"type": "string", "minLength": 1, "maxLength": 120}
            if info["type"] == "number":
                row_values = [value for value in numeric_values if value not in named_metric_bounds or value in named_field_bounds]
                if not row_values:
                    continue
                value = {"type": "number", "enum": row_values}
            if info["values"] is not None:
                values = _grounded_values(info, question)
                if not values:
                    continue
                value = {"type": "string", "enum": values}
            elif info["type"] == "string":
                spans = [value for value in literals if value not in bindings or token in bindings[value]]
                if not spans:
                    continue
                value = {"type": "string", "enum": sorted(spans)}
            scalar_ops = _OPS if info["type"] == "number" else ["eq", "ne"]
            choices.append(_object({"field": {"const": token}, "op": {"enum": [op for op in scalar_ops if op != "in"]}, "value": value}))
            choices.append(_object({"field": {"const": token}, "op": {"const": "in"}, "value": {"type": "array", "items": value, "minItems": 1, "maxItems": 10}}))
        return {"type": "array", "items": {"anyOf": choices} if choices else {"type": "null"}, "maxItems": 10 if choices else 0}

    exists_choices = [_object({"relation": {"const": token}, "negate": {"type": "boolean"},
                                "filters": predicates(info["fields"])}) for token, info in relations.items() if _relationship_cue(question, info)]
    aggregate_values = [value for value in numeric_values if value not in named_field_bounds or value in named_metric_bounds]
    having_item = _object({"metric": {"enum": list(metrics)}, "op": {"enum": ["eq", "gt", "gte", "lt", "lte"]},
                           "value": {"type": "number", "enum": aggregate_values}}) if aggregate_values else {"type": "null"}
    group_tokens = [token for token, info in dimensions.items() if not info.get("grouping_only") or _monthly_grouping_cue(question)] if _grouping_cue(question, dimensions, bindings) else []
    return _object({
        "metrics": {"type": "array", "items": {"enum": list(metrics)}, "minItems": 1, "maxItems": 3},
        "filters": predicates({**dimensions, **fields}),
        "having": {"type": "array", "items": having_item, "maxItems": 3 if aggregate_values else 0},
        "exists": {"anyOf": [*exists_choices, {"type": "null"}]},
        "comparison": nullable(_object({"kind": {"const": "above_average"}, "metric": {"enum": list(metrics)}})),
        "dimensions": {"type": "array", "items": {"enum": group_tokens} if group_tokens else {"type": "null"}, "maxItems": 2 if group_tokens else 0},
        "population": {"enum": ["primary", "all"]}, "set_operation": {"const": "union" if _dedup_requested(question) else "union_all"},
        "time_expression": {"enum": [None, *_time_candidates(question)]},
        "sort": nullable(_object({"field": {"enum": [*metrics, *dimensions]}, "direction": {"enum": ["asc", "desc"]}})),
        "limit": {"anyOf": [{"type": "null"}, {"type": "integer", "minimum": 1, "maximum": 100}]},
        "unresolved": {"type": "array", "items": _object({"span": {"type": "string", "minLength": 1, "maxLength": 120}, "reason": {"enum": _REASONS}}), "maxItems": 5},
    })


def _grounded(span: str, question: str) -> bool:
    return bool(re.search(r"(?<!\w)" + re.escape(re.sub(r"\s+", " ", span.strip()).casefold()) + r"(?!\w)",
                          re.sub(r"\s+", " ", question).casefold()))


def _demonstrations(public: dict) -> list[dict]:
    """Catalog-derived contract examples, independent of evaluation fixtures."""
    metric = public["metrics"][0]
    base = {"metrics": [metric["id"]], "dimensions": [], "filters": [], "having": [], "exists": None,
            "comparison": None, "population": "primary", "set_operation": "union_all", "time_expression": None,
            "sort": None, "limit": None, "unresolved": []}
    examples = [{"question": f"Total {metric['label']}", "intent": base}]
    group = next((item for item in public["dimensions"] if not item.get("grouping_only")), None)
    if group:
        examples.append({"question": f"{metric['label']} by {group['label']} with {metric['label']} above 250", "intent": {
            **base, "dimensions": [group["id"]], "having": [{"metric": metric["id"], "op": "gt", "value": 250}]}})
        examples.append({"question": f"{group['label']} groups with above-average {metric['label']}", "intent": {
            **base, "dimensions": [group["id"]], "comparison": {"kind": "above_average", "metric": metric["id"]}}})
        filter_field = next((item for item in public["dimensions"] if item != group and not item.get("grouping_only")), group)
        value = filter_field.get("values", ["Example"])[0]
        examples.append({"question": f"{metric['label']} by {group['label']} for {filter_field['label']} {value}", "intent": {
            **base, "dimensions": [group["id"]], "filters": [{"field": filter_field["id"], "op": "eq", "value": value}]}})
    if public["relations"]:
        relation = public["relations"][0]
        examples.append({"question": f"{metric['label']} with {relation['label']}", "intent": {
            **base, "exists": {"relation": relation["id"], "negate": False, "filters": []}}})
    month = next((item for item in public["dimensions"] if item.get("grouping_only")), None)
    if month:
        examples.append({"question": f"Monthly {metric['label']} last year", "intent": {
            **base, "dimensions": [month["id"]], "time_expression": "last year"}})
    examples.append({"question": f"Total {metric['label']} from current plus historical records, deduplicated", "intent": {
        **base, "population": "all", "set_operation": "union"}})
    key_order = ["metrics", "filters", "having", "exists", "comparison", "dimensions", "population", "set_operation", "time_expression", "sort", "limit", "unresolved"]
    return [{**example, "intent": {key: example["intent"][key] for key in key_order}} for example in examples]


def _number_grounded(value: Any, question: str) -> bool:
    if type(value) not in (int, float) or not -1e12 <= value <= 1e12:
        return False
    return any(float(token.replace(",", "")) == value
               for token in re.findall(r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?(?![\w.])", question))


def _predicates(items: Any, fields: dict, question: str, literal_candidates: list[str] | None = None,
                literal_bindings: dict[str, set[str]] | None = None) -> list[dict]:
    if not isinstance(items, list) or len(items) > 10:
        raise SemanticClarification("Use at most ten approved filters.")
    output = []
    for item in items:
        if not isinstance(item, dict) or set(item) != {"field", "op", "value"}:
            raise SemanticClarification("The model returned an invalid filter.")
        token, op, value = item["field"], item["op"], item["value"]
        if not isinstance(token, str) or token not in fields or op not in _OPS:
            raise SemanticClarification("The model selected an unapproved filter field or operation.")
        info = fields[token]
        if info.get("grouping_only") or (info["type"] != "number" and op not in {"eq", "ne", "in"}):
            raise SemanticClarification("This filter is incompatible with its approved field.")
        values = value if op == "in" else [value]
        if not isinstance(values, list) or not 1 <= len(values) <= 10 or (op != "in" and isinstance(value, list)):
            raise SemanticClarification("An IN filter needs a bounded list of values.")
        for single in values:
            if info["type"] == "number":
                if not _number_grounded(single, question):
                    raise SemanticClarification("The model invented a numeric filter value.")
            elif not isinstance(single, str) or not 1 <= len(single) <= 120:
                raise SemanticClarification("The model returned an invalid categorical filter.")
            elif info["values"] is not None:
                if single not in _grounded_values(info, question):
                    raise SemanticClarification("The model selected a filter value absent from the question or approved catalog.")
            elif not _grounded(single, question):
                raise SemanticClarification("The model invented a literal filter value.")
            elif literal_candidates is not None and single not in literal_candidates:
                raise SemanticClarification("The model used a catalog label or grammatical clause as a literal value.")
            elif literal_bindings and single in literal_bindings and token not in literal_bindings[single]:
                raise SemanticClarification("The model attached a literal value to a different explicitly named field.")
        output.append({"field": info["id"], "op": op, "value": value})
    # X AND X has exactly the same meaning as X, including SQL NULL semantics.
    # Remove only byte-for-byte equivalent typed predicates, preserving order.
    unique = {}
    for item in output:
        unique.setdefault(json.dumps(item, sort_keys=True), item)
    return list(unique.values())


def _validate_ir(ir: dict, public: dict, metrics: dict, dimensions: dict, fields: dict,
                 relations: dict, question: str, as_of: date | str | None) -> dict:
    required = {"metrics", "dimensions", "filters", "having", "exists", "comparison", "population",
                "set_operation", "time_expression", "sort", "limit", "unresolved"}
    if not isinstance(ir, dict) or set(ir) != required:
        raise SemanticClarification("The model returned unsupported relational intent fields.")
    unresolved = ir["unresolved"]
    if not isinstance(unresolved, list) or len(unresolved) > 5:
        raise SemanticClarification("The model returned invalid unresolved clauses.")
    for item in unresolved:
        if (not isinstance(item, dict) or set(item) != {"span", "reason"} or item["reason"] not in _REASONS
                or not isinstance(item["span"], str) or not 1 <= len(item["span"]) <= 120 or not _grounded(item["span"], question)):
            raise SemanticClarification("The model returned an unresolved clause absent from the question.")
    if unresolved:
        raise SemanticClarification(f"Cannot resolve '{unresolved[0]['span']}' within the approved relational catalog.")
    for name, allowed, lower, upper in (("metrics", metrics, 1, 3), ("dimensions", dimensions, 0, 2)):
        chosen = ir[name]
        if (not isinstance(chosen, list) or not lower <= len(chosen) <= upper or
                any(not isinstance(token, str) or token not in allowed for token in chosen) or len(set(chosen)) != len(chosen)):
            raise SemanticClarification("Choose up to three approved metrics and two grouping dimensions.")
    labels = [item["label"] for kind in ("metrics", "dimensions", "fields", "relations") for item in public[kind]]
    literals = _literal_candidates(question, labels)
    available = {**dimensions, **fields}
    for relation in relations.values():
        available.update(relation["fields"])
    bindings = _literal_bindings(question, available, literals)
    if ir["dimensions"] and not _grouping_cue(question, dimensions, bindings):
        raise SemanticClarification("The model added a breakdown that was not requested. A total must remain a total.")
    raw_predicates = [*ir["filters"], *(ir["exists"].get("filters", []) if isinstance(ir["exists"], dict) else [])]
    for literal, allowed_tokens in bindings.items():
        if not any(isinstance(predicate, dict) and predicate.get("field") in allowed_tokens and
                   literal in (predicate.get("value") if isinstance(predicate.get("value"), list) else [predicate.get("value")])
                   for predicate in raw_predicates):
            raise SemanticClarification("The model omitted a literal condition attached to an explicitly named field.")
    if any(dimensions[token].get("grouping_only") for token in ir["dimensions"]) and not _monthly_grouping_cue(question):
        raise SemanticClarification("The model added a monthly breakdown that was not requested.")
    filters = _predicates(ir["filters"], {**dimensions, **fields}, question, literals, bindings)
    having = ir["having"]
    if not isinstance(having, list) or len(having) > 3:
        raise SemanticClarification("Use at most three aggregate thresholds.")
    mapped_having = []
    for item in having:
        if (not isinstance(item, dict) or set(item) != {"metric", "op", "value"} or item["metric"] not in ir["metrics"]
                or item["op"] not in {"eq", "gt", "gte", "lt", "lte"} or not _number_grounded(item["value"], question)):
            raise SemanticClarification("The model returned an ungrounded aggregate threshold.")
        mapped_having.append({**item, "metric": metrics[item["metric"]]})
    if having and not ir["dimensions"]:
        raise SemanticClarification("Aggregate group thresholds need an approved grouping.")
    exists = ir["exists"]
    if exists is not None:
        if (not isinstance(exists, dict) or set(exists) != {"relation", "negate", "filters"}
                or exists["relation"] not in relations or type(exists["negate"]) is not bool):
            raise SemanticClarification("The model returned an unapproved relationship.")
        info = relations[exists["relation"]]
        if not _relationship_cue(question, info):
            raise SemanticClarification("The model added a related-event condition that was not requested.")
        exists = {"relation": info["id"], "negate": exists["negate"],
                  "filters": _predicates(exists["filters"], info["fields"], question, literals, bindings)}
    comparison = ir["comparison"]
    if comparison is not None:
        if (not isinstance(comparison, dict) or set(comparison) != {"kind", "metric"}
                or comparison["kind"] != "above_average" or comparison["metric"] not in ir["metrics"]
                or not ir["dimensions"] or having):
            raise SemanticClarification("An above-average comparison needs groups and one selected metric, without another aggregate threshold.")
        comparison = {"kind": "above_average", "metric": metrics[comparison["metric"]]}
    if ir["population"] not in {"primary", "all"} or ir["set_operation"] not in {"union", "union_all"}:
        raise SemanticClarification("Choose a supported record population and set operation.")
    if ir["set_operation"] == "union" and ir["population"] != "all":
        raise SemanticClarification("Deduplication is supported when combining current and archived populations.")
    expression = ir["time_expression"]
    candidates = _time_candidates(question)
    if expression is not None and (not isinstance(expression, str) or expression not in candidates):
        raise SemanticClarification("The model invented a date expression.")
    if candidates and (len(candidates) != 1 or expression != candidates[0]):
        raise SemanticClarification("The model did not preserve the entire date constraint.")
    date_from, date_to = resolve_time(expression, as_of)
    sorting = ir["sort"]
    if sorting is not None:
        if (not isinstance(sorting, dict) or set(sorting) != {"field", "direction"}
                or sorting["field"] not in [*ir["metrics"], *ir["dimensions"]]
                or sorting["direction"] not in {"asc", "desc"}):
            raise SemanticClarification("Sort by one selected metric or grouping.")
    limit = ir["limit"]
    if limit is not None and (type(limit) is not int or not 1 <= limit <= 100 or not ir["dimensions"]):
        raise SemanticClarification("A top/bottom limit requires grouping and must be 1-100.")
    _validate_constraints(question, ir, public, dimensions, fields, relations)
    if sorting is None:
        month = next((token for token in ir["dimensions"] if dimensions[token].get("grouping_only")), None) if len(ir["dimensions"]) == 1 else None
        sorting = {"field": month or ir["metrics"][0], "direction": "asc" if month else "desc"}
    all_ids = {**metrics, **{token: info["id"] for token, info in dimensions.items()}}
    return {"version": 2, "metrics": [metrics[token] for token in ir["metrics"]],
            "dimensions": [dimensions[token]["id"] for token in ir["dimensions"]],
            "filters": filters, "having": mapped_having, "exists": exists, "comparison": comparison,
            "population": ir["population"], "set_operation": ir["set_operation"],
            "date_from": date_from, "date_to": date_to,
            "sort": {"field": all_ids[sorting["field"]], "direction": sorting["direction"]}, "limit": limit or 100}


def _validate_constraints(question: str, ir: dict, public: dict, dimensions: dict, fields: dict, relations: dict) -> None:
    """Fail closed for explicit omitted clauses; never synthesize model intent."""
    text = question.casefold()
    if re.search(r"\b(?:forecast|predict|median|percentile|intersect|except\s+select|window function|running total)\b", text):
        raise SemanticClarification("The request contains a calculation outside the approved relational contract.")
    archived = bool(re.search(r"\b(?:archiv\w*|historical|history)\b", text))
    if archived != (ir["population"] == "all"):
        raise SemanticClarification("The model did not preserve the requested current/archive population.")
    if archived and not re.search(r"\b(?:current|both|all|combined?|including|together|union)\b", text):
        raise SemanticClarification("Specify current records or current and archived records together.")
    deduplicate = _dedup_requested(question)
    if deduplicate != (ir["set_operation"] == "union"):
        raise SemanticClarification("The model did not preserve the requested duplicate handling.")
    above = bool(re.search(r"\babove[ -](?:the )?(?:overall |group |grouped )?average\b|\bgreater than (?:the )?(?:overall |group )?average\b", text))
    if above != (ir["comparison"] is not None):
        raise SemanticClarification("The model did not preserve the above-average comparison.")
    # 'at least 20' constrains a value; it is not a request for lowest-first sorting.
    rank_question = re.sub(r"\bat (?:least|most)\s+[$\u20ac\u00a3]?\s*\d[\d,.]*", "numeric bound", question, flags=re.I)
    directions = _ranking_directions(rank_question)
    if len(directions) > 1:
        raise SemanticClarification("Choose one consistent ranking direction.")
    if directions:
        direction = next(iter(directions))
        sorting = ir["sort"]
        if sorting is None or sorting["direction"] != ("desc" if direction == "value_desc" else "asc"):
            raise SemanticClarification("The model did not preserve the requested ranking direction.")
    ranks = re.findall(r"\b(top|bottom)\s+(\d+)\b", text)
    if ranks and (len(ranks) != 1 or ir["limit"] != int(ranks[0][1])):
        raise SemanticClarification("The model did not preserve the requested ranking limit.")
    all_predicates = [*ir["filters"], *ir["having"], *(ir["exists"]["filters"] if ir["exists"] else [])]
    logical_text = re.sub(r"\bon or (?:after|before)\b", "date bound", text)
    for alternative in re.finditer(r"\bor\b", logical_text):
        represented = False
        for predicate in ir["filters"]:
            if predicate["op"] != "in":
                continue
            positions = [(match.start(), match.end()) for value in predicate["value"]
                         for match in re.finditer(r"(?<!\w)" + re.escape(str(value).casefold()) + r"(?!\w)", logical_text)]
            if any(end <= alternative.start() and alternative.start() - end <= 5 for _, end in positions) and any(
                    start >= alternative.end() and start - alternative.end() <= 5 for start, _ in positions):
                represented = True
        if not represented:
            raise SemanticClarification("OR is supported for alternative values of one field using IN. Separate conditions across different fields.")
    operators = {"at least": "gte", "at most": "lte", "more than": "gt", "greater than": "gt", "above": "gt",
                 "exceed": "gt", "exceeds": "gt", "exceeding": "gt", "over": "gt", "less than": "lt", "below": "lt", "under": "lt",
                 "greater than or equal to": "gte", "less than or equal to": "lte"}
    alternatives = "|".join(re.escape(op) for op in sorted(operators, key=len, reverse=True))
    for bound in re.finditer(r"\b(" + alternatives + r")\s+[$\u20ac\u00a3]?\s*(\d[\d,]*(?:\.\d+)?)\b", text):
        if re.search(r"\bnot\s+$", text[:bound.start()]):
            raise SemanticClarification("Write a negated numeric bound explicitly, such as at most or at least.")
        value = float(bound[2].replace(",", ""))
        if not any(predicate["op"] == operators[bound[1]] and predicate["value"] == value for predicate in all_predicates):
            raise SemanticClarification("The model omitted or reversed an explicit numeric threshold.")
        labels = [(match.end(), item["id"], kind) for kind in ("metrics", "fields") for item in public[kind]
                  for match in re.finditer(r"(?<!\w)" + re.escape(item["label"].casefold()) + r"(?!\w)", text[:bound.start()])]
        if labels:
            end, token, kind = max(labels)
            if re.fullmatch(r"\s*(?:is|are|was|were|total|totals|of|must be|should be|is strictly)?\s*", text[end:bound.start()]):
                predicates = ir["having"] if kind == "metrics" else ir["filters"]
                key = "metric" if kind == "metrics" else "field"
                if not any(predicate[key] == token and predicate["op"] == operators[bound[1]] and predicate["value"] == value for predicate in predicates):
                    raise SemanticClarification("The model attached a threshold to a different field or aggregation level.")
    # Approved categorical values must not disappear between the question and IR.
    used = [*ir["filters"], *(ir["exists"]["filters"] if ir["exists"] else [])]
    used_values = [value for item in used for value in (item["value"] if isinstance(item["value"], list) else [item["value"]])]
    available = {**dimensions, **fields}
    for info in relations.values():
        available.update(info["fields"])
    for info in available.values():
        if info["values"] is not None:
            grounded = _grounded_values(info, question)
            if any(value not in used_values for value in grounded):
                raise SemanticClarification("The model omitted an explicitly requested categorical filter.")
    # Exact approved metric labels are policy evidence, not a language parser.
    matches = [(match.start(), match.end(), item["id"]) for item in public["metrics"]
               for match in re.finditer(r"(?<!\w)" + re.escape(item["label"].casefold()) + r"(?!\w)", text)]
    for start, end, token in matches:
        # 'for Paid invoices' constrains a population; it does not request the
        # distinct-Invoices metric. This check only exempts that explicit shape.
        entity_filter = any(re.search(r"(?<!\w)" + re.escape(str(value).casefold()) + r"\s+$", text[:start])
                            for value in used_values)
        if not any(other_start <= start and end <= other_end and (start, end) != (other_start, other_end)
                   for other_start, other_end, _ in matches) and token not in ir["metrics"] and not entity_filter:
            raise SemanticClarification("The model omitted an explicitly requested approved metric.")
    for grouping in re.finditer(r"\b(?:by|per|each|every)\s+(.+?)(?=\b(?:where|with|without|for|in|whose|from|having)\b|$)", text):
        named_groups = set()
        remaining = grouping[1]
        for item in public["dimensions"]:
            label = item["label"].casefold()
            variants = {label, label + "s", label[:-1] + "ies" if label.endswith("y") else label}
            if any(_grounded(variant, grouping[1]) for variant in variants) and item["id"] not in ir["dimensions"]:
                raise SemanticClarification("The model omitted an explicitly requested grouping dimension.")
            for variant in sorted(variants, key=len, reverse=True):
                if _grounded(variant, grouping[1]):
                    named_groups.add(item["id"])
                    remaining = re.sub(r"(?<!\w)" + re.escape(variant) + r"(?!\w)", " ", remaining)
        if named_groups and re.fullmatch(r"[\s,&.]*|[\s,&.]*and[\s,&.]*", remaining) and set(ir["dimensions"]) != named_groups:
            raise SemanticClarification("The model added an unrequested grouping to an explicit breakdown.")
        suffixes: dict[str, list[str]] = {}
        for item in public["dimensions"]:
            words = item["label"].casefold().split()
            if len(words) > 1:
                suffixes.setdefault(words[-1], []).append(item["label"].casefold())
        for suffix, labels in suffixes.items():
            if len(labels) > 1 and _grounded(suffix, grouping[1]) and not any(_grounded(label, grouping[1]) for label in labels):
                raise SemanticClarification("Specify which approved grouping you mean: " + " or ".join(labels) + ".")
    for token, relation in relations.items():
        label = relation["label"].casefold()
        if _grounded(label, question):
            if ir["exists"] is None or ir["exists"]["relation"] != token:
                raise SemanticClarification("The model omitted an explicitly requested related-event condition.")
            negative = bool(re.search(r"\b(?:without|no|not having|never had|excluding)\s+(?:\w+\s+){0,2}" + re.escape(label) + r"\b", text))
            if ir["exists"]["negate"] != negative:
                raise SemanticClarification("The model reversed the related-event condition.")


class RelationalSemanticParser(SemanticParser):
    def __init__(self, config: SemanticConfig | None = None, shared_parser: SemanticParser | None = None):
        selected = config or SemanticConfig.from_env()
        super().__init__(replace(selected, max_tokens=max(768, selected.max_tokens)))
        if shared_parser is not None:
            self._inference_gate = shared_parser._inference_gate

    def parse(self, question: str, catalog: dict, as_of: date | str | None = None) -> SemanticResult:
        question = validate_question(question)
        public, metrics, dimensions, fields, relations = _project_catalog(catalog)
        material = [question, public, metrics, dimensions, fields, relations, str(as_of),
                    catalog.get("dataset", {}).get("id"), catalog.get("dataset", {}).get("catalog_version"), RELATIONAL_CONTRACT_VERSION,
                    hashlib.sha256(_SYSTEM_PROMPT.encode()).hexdigest(), self.config.model, self.config.endpoint]
        key = hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()
        with self._lock:
            cached = self._cache.get(key)
            if cached:
                self._cache.move_to_end(key)
                result = copy.deepcopy(cached)
                result.telemetry = {**_empty_telemetry(), "model": cached.telemetry["model"], "semantic_cache_hit": True,
                                    "interpretation_cache_hit": True, "interpretation_source": "semantic_cache"}
                return result
        user = json.dumps({"catalog": public, "contract_examples": _demonstrations(public), "time_candidates": _time_candidates(question), "question": question}, ensure_ascii=False, separators=(",", ":"))
        if len(_SYSTEM_PROMPT) + len(user) > self.config.max_prompt_chars:
            raise SemanticClarification("The relational catalog exceeds the bounded local prompt budget.")
        payload = {"model": self.config.model, "messages": [{"role": "system", "content": _SYSTEM_PROMPT}, {"role": "user", "content": user}],
                   "temperature": 0, "seed": 42, "max_tokens": self.config.max_tokens, "stream": False,
                   "response_format": {"type": "json_object", "schema": _schema(metrics, dimensions, fields, relations, question, public)}, "cache_prompt": True}
        if not self._inference_gate.acquire(timeout=2):
            raise ModelUnavailable("The local model is busy with another question. Try again shortly; the query builder remains available.")
        telemetry = {**_empty_telemetry(), "model_calls": 1, "model": self.config.model,
                     "input_tokens": None, "output_tokens": None, "prompt_tokens": None,
                     "completion_tokens": None, "total_tokens": None, "interpretation_source": "local_model"}
        started = time.perf_counter()
        try:
            response = self._request(payload)
            telemetry["model_latency_ms"] = round((time.perf_counter() - started) * 1000, 2)
            usage = response.get("usage", {})
            for source, target in (("prompt_tokens", "input_tokens"), ("completion_tokens", "output_tokens")):
                value = usage.get(source)
                telemetry[source] = telemetry[target] = value if type(value) is int and value >= 0 else None
            telemetry["total_tokens"] = sum((telemetry["input_tokens"], telemetry["output_tokens"])) if all(type(telemetry[key]) is int for key in ("input_tokens", "output_tokens")) else None
            if isinstance(response.get("model"), str) and 0 < len(response["model"]) <= 200:
                telemetry["model"] = response["model"]
            choice = response["choices"][0]
            if choice.get("finish_reason") == "length":
                raise SemanticClarification("The local model reached its response budget. Split this analysis or use the query builder.")
            content = choice["message"]["content"]
            if not isinstance(content, str) or len(content) > 16000:
                raise ValueError("Invalid content")
            ir = json.loads(content)
            plan = _validate_ir(ir, public, metrics, dimensions, fields, relations, question, as_of)
        except (ModelUnavailable, SemanticClarification) as exc:
            telemetry["model_latency_ms"] = round((time.perf_counter() - started) * 1000, 2)
            exc.telemetry = telemetry
            raise
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
            raise SemanticClarification("The local model returned an invalid relational intent. Rephrase or use the query builder.", telemetry) from exc
        finally:
            self._inference_gate.release()
        result = SemanticResult(plan, ir, telemetry)
        if self.config.cache_size:
            with self._lock:
                self._cache[key] = copy.deepcopy(result)
                while len(self._cache) > self.config.cache_size:
                    self._cache.popitem(last=False)
        return result
