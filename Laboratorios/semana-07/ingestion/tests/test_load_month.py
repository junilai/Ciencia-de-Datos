import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import load_month as lm  # noqa: E402

FLOW = Path(__file__).resolve().parents[2] / "flows" / "nyc_taxi_elt.yml"


def test_expected_periods_cover_20_months():
    periods = lm.expected_periods()
    assert len(periods) == 20
    assert periods[0] == "2025-01"
    assert periods[11] == "2025-12"
    assert periods[-1] == "2026-08"


@pytest.mark.parametrize("period", ["2025-01", "2026-12"])
def test_validate_period_accepts_yyyy_mm(period):
    assert lm.validate_period(period) == period


@pytest.mark.parametrize("period", ["2025-1", "2025-13", "25-01", "2025/01", ""])
def test_validate_period_rejects_bad_format(period):
    with pytest.raises(ValueError):
        lm.validate_period(period)


def test_source_url():
    assert lm.source_url("2025-03") == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-03.parquet"
    )


class FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code


class FakeSession:
    def __init__(self, status_code):
        self.status_code = status_code

    def head(self, url, timeout, allow_redirects):
        return FakeResponse(self.status_code)


def test_is_published_true_on_200():
    assert lm.is_published("u", FakeSession(200)) is True


@pytest.mark.parametrize("status", [403, 404])
def test_is_published_false_when_not_yet_published(status):
    assert lm.is_published("u", FakeSession(status)) is False


def test_is_published_raises_on_unexpected_status():
    with pytest.raises(RuntimeError):
        lm.is_published("u", FakeSession(500))


def test_verify_row_count_ok():
    lm.verify_row_count(10, 10, "2025-01")


def test_verify_row_count_mismatch_raises():
    with pytest.raises(RuntimeError, match="2025-01"):
        lm.verify_row_count(10, 9, "2025-01")


@pytest.mark.skipif(not FLOW.exists(), reason="el flow se crea en F4")
def test_flow_periods_match_expected():
    flow = yaml.safe_load(FLOW.read_text())
    ingest = next(t for t in flow["tasks"] if t["id"] == "ingest")
    assert ingest["values"] == lm.expected_periods()
