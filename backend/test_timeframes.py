"""Structured periods resolve against the source reference date, never the wall clock."""
import pytest

from backend.core.llm import SemanticClarification
from backend.core.timeframes import TimeframeError, resolve_timeframe

AS_OF = "2025-12-31"


@pytest.mark.parametrize("spec,expected", [
    ({"type": "calendar", "year": 2025, "quarter": 2, "month": None}, ("2025-04-01", "2025-06-30")),
    ({"type": "calendar", "year": 2024, "month": 2}, ("2024-02-01", "2024-02-29")),
    ({"type": "calendar", "year": 2024}, ("2024-01-01", "2024-12-31")),
    ({"type": "relative", "unit": "month", "offset": -1}, ("2025-11-01", "2025-11-30")),
    ({"type": "relative", "unit": "month", "offset": 0}, ("2025-12-01", "2025-12-31")),
    ({"type": "relative", "unit": "quarter", "offset": -1}, ("2025-07-01", "2025-09-30")),
    ({"type": "relative", "unit": "year", "offset": -1}, ("2024-01-01", "2024-12-31")),
    ({"type": "relative", "unit": "week", "offset": -1}, ("2025-12-22", "2025-12-28")),
    ({"type": "trailing", "unit": "day", "count": 7}, ("2025-12-25", "2025-12-31")),
    ({"type": "trailing", "unit": "month", "count": 3}, ("2025-10-01", "2025-12-31")),
    ({"type": "range", "start": "2025-10-01", "end": None}, ("2025-10-01", None)),
    ({"type": "range", "start": None, "end": "2025-02-28"}, (None, "2025-02-28")),
])
def test_periods_resolve_exactly(spec, expected):
    assert resolve_timeframe(spec, AS_OF) == expected


def test_current_period_ends_at_reference_date():
    assert resolve_timeframe({"type": "relative", "unit": "month", "offset": 0}, "2026-09-12") == ("2026-09-01", "2026-09-12")


@pytest.mark.parametrize("spec", [
    None, {"type": "calendar", "year": 2025, "quarter": 2, "month": 5}, {"type": "calendar", "year": "2025"},
    {"type": "relative", "unit": "fortnight", "offset": -1}, {"type": "relative", "unit": "month", "offset": True},
    {"type": "trailing", "unit": "day", "count": 0}, {"type": "range", "start": None, "end": None},
    {"type": "range", "start": "2025-02-30", "end": None}, {"type": "range", "start": "2025-06-01", "end": "2025-01-01"},
    {"type": "since last christmas"},
])
def test_malformed_periods_are_repairable_errors(spec):
    with pytest.raises(TimeframeError):
        resolve_timeframe(spec, AS_OF)


def test_relative_period_needs_a_reference_date():
    with pytest.raises(SemanticClarification, match="reference date"):
        resolve_timeframe({"type": "relative", "unit": "month", "offset": -1}, None)


@pytest.mark.parametrize("spec", [{"type": "relative", "unit": "month", "offset": 1}, {"type": "calendar", "year": 2030}])
def test_future_periods_are_refused_as_forecasts(spec):
    with pytest.raises(SemanticClarification, match="Forecasts"):
        resolve_timeframe(spec, AS_OF)
