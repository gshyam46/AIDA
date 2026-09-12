"""Keep the transfer evaluator from crediting wrong meanings or failed models."""
import copy
import importlib.util
from pathlib import Path

import pytest

_path = Path(__file__).resolve().parents[1] / "scripts/evaluate_blind.py"
_spec = importlib.util.spec_from_file_location("aida_blind_evaluator", _path)
evaluation = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(evaluation)

PLAN = {"version": 2, "metrics": ["weight", "count"], "dimensions": ["zone"],
        "filters": [{"field": "state", "op": "in", "value": ["Open", "Closed"]}, {"field": "quantity", "op": "gte", "value": 2}],
        "having": [], "population": "primary", "set_operation": "union_all", "exists": None,
        "comparison": None, "date_from": None, "date_to": None,
        "sort": {"field": "weight", "direction": "desc"}, "limit": 100}
ROWS = [{"zone": "North", "weight": 100.1, "count": 3}]


def outcome(expected="answer", **result):
    case = {"expected": expected, "plan": PLAN if expected == "answer" else None}
    return evaluation.classify(case, result, ROWS if expected == "answer" else None)


def test_correct_result_requires_both_semantics_and_independent_rows():
    assert outcome(success=True, plan=PLAN, data=ROWS)["category"] == "correct_answer"
    wrong = copy.deepcopy(PLAN)
    wrong["population"] = "all"
    assert outcome(success=True, plan=wrong, data=ROWS)["category"] == "wrong_accepted_meaning"


def test_equivalent_and_and_in_order_is_not_a_semantic_failure():
    equivalent = copy.deepcopy(PLAN)
    equivalent["metrics"].reverse()
    equivalent["filters"][0]["value"].reverse()
    equivalent["filters"].reverse()
    judged = outcome(success=True, plan=equivalent, data=ROWS)
    assert judged["category"] == "correct_answer" and not judged["exact_plan"]


@pytest.mark.parametrize("field,value", [("limit", 5), ("date_to", "2026-01-01"), ("sort", {"field": "weight", "direction": "asc"})])
def test_result_selection_changes_do_not_pass_on_coincidental_rows(field, value):
    wrong = copy.deepcopy(PLAN)
    wrong[field] = value
    assert outcome(success=True, plan=wrong, data=ROWS)["category"] == "wrong_accepted_meaning"


def test_database_mismatch_is_not_mislabeled_semantic_failure():
    assert outcome(success=True, plan=PLAN, data=[])["category"] == "execution_mismatch"


def test_safe_refusal_and_false_refusal_are_separate():
    failed = {"success": False, "error_type": "clarification_required", "data": []}
    assert outcome("refusal", **failed)["category"] == "correct_refusal"
    assert outcome(**failed)["category"] == "false_refusal"


def test_model_outage_is_never_credited_as_refusal():
    assert outcome("refusal", success=False, error_type="model_unavailable", data=[])["category"] == "availability_failure"


def test_executable_answer_to_unsupported_question_is_unsafe_acceptance():
    assert outcome("refusal", success=True, plan=PLAN, data=ROWS)["category"] == "unsafe_acceptance"


def test_numerical_tolerance_preserves_missing_and_boolean_types():
    assert evaluation.equivalent_value(0.1 + 0.2, .3)
    assert not evaluation.equivalent_value(None, 0)
    assert not evaluation.equivalent_value(True, 1)
    assert not evaluation.equivalent_value(100.01, 100.02)


def test_privacy_audit_catches_mapping_and_sensitive_canary_leakage():
    payload = {"messages": [{"content": "system"}, {"content": '{"catalog":{"metrics":[{"id":"m0","table":"shipment_legs"}]},"question":"weight"}'}]}
    audited = evaluation.privacy_audit(payload, {"shipment_legs"}, ["FAKE_ONLY_SENSITIVE_VALUE"])
    assert not audited["passed"] and audited["forbidden_keys"] == ["table"]


def test_unrequested_order_is_presentation_but_requested_order_is_meaning():
    case = {"expected": "answer", "plan": PLAN, "order_sensitive": False}
    rows = [*ROWS, {"zone": "South", "weight": 80.0, "count": 2}]
    changed = copy.deepcopy(PLAN)
    changed["sort"]["direction"] = "asc"
    result = {"success": True, "plan": changed, "data": list(reversed(rows))}
    judged = evaluation.classify(case, result, rows)
    assert judged["category"] == "correct_answer" and not judged["ordered_rows_match"]
    case["order_sensitive"] = True
    assert evaluation.classify(case, result, rows)["category"] == "wrong_accepted_meaning"
