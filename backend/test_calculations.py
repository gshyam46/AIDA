"""Post-aggregation calculations are exact, bounded and replayable from saved plans."""
import pytest

from backend.core import calculations
from backend.core.hybrid import HybridAnalytics
from backend.core.relational_demo import ensure_relational_demos, relational_plan
from backend.core.sources import SourceRegistry


def spec(op, inputs, identifier="calculation_1"):
    return {"id": identifier, "label": "Result", "op": op, "inputs": inputs}


def test_arithmetic_and_division_by_zero():
    rows = [{"g": "a", "x": 10, "y": 4}, {"g": "b", "x": 3, "y": 0}, {"g": "c", "x": None, "y": 2}]
    ratio = calculations.apply(rows, [spec("ratio", ["x", "y"])], {}, lambda key: key, {}, "g")
    assert [row["calculation_1"] for row in ratio] == [2.5, None, None]
    difference = calculations.apply(rows, [spec("difference", ["x", "y"])], {}, lambda key: key, {}, "g")
    assert [row["calculation_1"] for row in difference] == [6, 3, None]


def test_share_running_total_and_change_follow_month_order():
    rows = [{"month": "2025-02", "value": 30}, {"month": "2025-01", "value": 10}, {"month": "2025-03", "value": 60}]
    share = calculations.apply(rows, [spec("share_of_total", ["value"])], {}, lambda key: "value", {"value": 100}, "month")
    assert [row["calculation_1"] for row in share] == [0.3, 0.1, 0.6]
    running = calculations.apply(rows, [spec("running_total", ["value"])], {}, lambda key: "value", {}, "month")
    assert {row["month"]: row["calculation_1"] for row in running} == {"2025-01": 10, "2025-02": 40, "2025-03": 100}
    change = calculations.apply(rows, [spec("percent_change", ["value"])], {}, lambda key: "value", {}, "month")
    assert {row["month"]: row["calculation_1"] for row in change} == {"2025-01": None, "2025-02": 2.0, "2025-03": 1.0}


def test_thresholds_sort_and_limit_apply_after_calculation():
    rows = [{"g": name, "x": x, "y": 1} for name, x in (("a", 5), ("b", 9), ("c", 1), ("d", 7))]
    post = {"thresholds": [{"target": "calculation_1", "op": "gt", "value": 2}], "sort": {"by": "calculation_1", "direction": "desc"}, "limit": 2}
    result = calculations.apply(rows, [spec("ratio", ["x", "y"])], post, lambda key: key, {}, "g")
    assert [row["g"] for row in result] == ["b", "d"]


@pytest.mark.parametrize("specs,post,plan", [
    ([spec("running_total", ["tickets"])], {}, {"metric": "tickets", "dimension": "team"}),
    ([spec("share_of_total", ["tickets"])], {}, {"metric": "tickets"}),
    ([spec("ratio", ["tickets", "tickets"])], {}, {"version": 2, "metrics": ["tickets"], "dimensions": ["team"]}),
    ([spec("ratio", ["revenue", "units"])], {}, {"version": 2, "metrics": ["revenue"], "dimensions": ["region"]}),
    ([spec("median", ["tickets"])], {}, {"metric": "tickets", "dimension": "team"}),
    ([{**spec("share_of_total", ["tickets"]), "id": "calc; DROP"}], {}, {"metric": "tickets", "dimension": "team"}),
    ([spec("share_of_total", ["tickets"])], {"sort": {"by": "tickets", "direction": "desc"}}, {"metric": "tickets", "dimension": "team"}),
    ([spec("share_of_total", ["tickets"])], {"limit": 1000}, {"metric": "tickets", "dimension": "team"}),
])
def test_invalid_calculation_specs_are_rejected(specs, post, plan):
    with pytest.raises(calculations.CalculationError):
        calculations.validate(specs, post, plan)


@pytest.fixture
def hybrid(tmp_path):
    registry = SourceRegistry(tmp_path)
    registry.register_support_demo()
    for source in ensure_relational_demos(registry.directory):
        registry.register_source(source["path"], source["manifest"], source["id"])
    return HybridAnalytics(registry, interpreter=object())


def test_saved_plan_calculations_replay_through_hybrid(hybrid):
    shares = hybrid.query(plan={"metric": "tickets", "dimension": "team", "calculations": [spec("share_of_total", ["tickets"])]}, source_id="support")
    assert shares["success"], shares
    assert sum(row["calculation_1"] for row in shares["data"]) == pytest.approx(1, abs=1e-5)
    assert shares["plan"]["calculations"][0]["op"] == "share_of_total"
    assert shares["calculations"][0]["format"] == "percent" and shares["meta"]["model_calls"] == 0
    monthly = hybrid.query(plan={"metric": "tickets", "dimension": "month", "calculations": [spec("running_total", ["tickets"])]}, source_id="support")
    values = [row["calculation_1"] for row in monthly["data"]]
    assert values == sorted(values) and values[-1] == 168


def test_relational_ratio_sorted_and_limited_after_sql(hybrid):
    plan = {**relational_plan(["revenue", "units"], ["category"]), "calculations": [spec("ratio", ["revenue", "units"])],
            "post": {"sort": {"by": "calculation_1", "direction": "desc"}, "limit": 2}}
    result = hybrid.query(plan=plan, source_id="warehouse")
    assert result["success"], result
    assert len(result["data"]) == 2
    assert all(row["calculation_1"] == round(row["revenue"] / row["units"], 6) for row in result["data"])
    assert result["data"][0]["calculation_1"] >= result["data"][1]["calculation_1"]
    assert "calculation" in result["lineage"]["operations"]


def test_invalid_saved_calculation_is_a_clarification_not_an_error(hybrid):
    result = hybrid.query(plan={"metric": "tickets", "dimension": "team", "calculations": [spec("running_total", ["tickets"])]}, source_id="support")
    assert not result["success"] and result["error_type"] == "clarification_required" and "sql" not in result
