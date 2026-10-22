"""Offline dashboard builder tests; no web server or Power BI runtime is required."""
import csv
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("build_dashboard", ROOT / "bi" / "build_dashboard.py")
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_dashboard_builder_creates_self_contained_three_page_html(tmp_path):
    root = tmp_path
    write_csv(root / "bi/page1_sales_funnel.csv", ["event_date", "view", "add_to_cart", "checkout", "purchase"], [
        {"event_date": "2026-07-01", "view": 100, "add_to_cart": 30, "checkout": 15, "purchase": 10},
        {"event_date": "2026-07-02", "view": 80, "add_to_cart": 20, "checkout": 10, "purchase": 5},
    ])
    write_csv(root / "bi/page2_cohort_retention.csv", ["cohort", "0", "1"], [
        {"cohort": "2026-07", "0": 1, "1": 0.5},
    ])
    write_csv(root / "bi/page3_abandoned_carts.csv", ["day", "device", "abandoned_sessions"], [
        {"day": "2026-07-01", "device": "mobile", "abandoned_sessions": 7},
    ])
    write_csv(root / "bi/dim_customer_segments.csv", ["customer_id", "tier", "segment", "ltv", "orders", "propensity", "recency_days", "abandon_rate"], [
        {"customer_id": 1, "tier": "gold", "segment": "Champions", "ltv": 125.5, "orders": 2, "propensity": 0.8, "recency_days": 10, "abandon_rate": 0.1},
    ])
    write_csv(root / "mysql/orders.csv", ["order_id", "total_amount"], [
        {"order_id": 1, "total_amount": 125.5},
    ])

    output = MODULE.build_dashboard(root)
    doc = output.read_text(encoding="utf-8")
    assert output.name == "dashboard.html"
    assert "Sales funnel" in doc
    assert "Cohort retention" in doc
    assert "03 · Abandoned carts &amp; segments" not in doc  # visible label is literal text, not HTML markup
    assert "03 · Abandoned carts & segments" in doc
    assert "Champions" in doc
    assert "125.50" in doc
    assert "const C360 =" in doc
    assert "https://" not in doc  # no external JS/CSS or network dependency


def test_dashboard_builder_fails_clearly_when_csv_exports_are_missing(tmp_path):
    try:
        MODULE.build_dashboard(tmp_path)
    except FileNotFoundError as exc:
        assert "No BI CSV exports found" in str(exc)
    else:
        raise AssertionError("Expected a clear missing-export exception")
