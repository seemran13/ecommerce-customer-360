#!/usr/bin/env python3
"""Build a self-contained, offline Customer360 dashboard from the BI CSV extracts.

This is a browser-based companion to the Power BI exports, not a .pbix replacement.
It has no external runtime dependencies and can be opened directly from disk.
"""
from __future__ import annotations

import argparse
import csv
import html
import json
from pathlib import Path
from statistics import mean


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def number(value: object, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def fmt_int(value: float) -> str:
    return f"{int(round(value)):,}"


def fmt_money(value: float) -> str:
    return f"${value:,.2f}"


def build_dashboard(data_root: Path) -> Path:
    bi_root = data_root / "bi"
    funnel = read_csv(bi_root / "page1_sales_funnel.csv")
    retention = read_csv(bi_root / "page2_cohort_retention.csv")
    abandoned = read_csv(bi_root / "page3_abandoned_carts.csv")
    segments = read_csv(bi_root / "dim_customer_segments.csv")
    if not any((funnel, retention, abandoned, segments)):
        raise FileNotFoundError(
            f"No BI CSV exports found under {bi_root}. Run bi/export_powerbi.py first."
        )

    event_types = sorted({key for row in funnel for key in row if key not in {"event_date", ""}})
    funnel_totals = {event: sum(number(row.get(event)) for row in funnel) for event in event_types}
    views = funnel_totals.get("view", 0.0)
    carts = funnel_totals.get("add_to_cart", 0.0)
    checkouts = funnel_totals.get("checkout", 0.0)
    purchases = funnel_totals.get("purchase", 0.0)
    cart_rate = carts / views if views else 0.0
    conversion_rate = purchases / views if views else 0.0

    orders = read_csv(data_root / "mysql" / "orders.csv")
    revenue = sum(number(row.get("total_amount")) for row in orders)
    customer_count = len(segments)
    abandoned_count = sum(number(row.get("abandoned_sessions")) for row in abandoned)
    avg_propensity = mean([number(row.get("propensity")) for row in segments]) if segments else 0.0
    avg_ltv = mean([number(row.get("ltv")) for row in segments]) if segments else 0.0

    # Canvas charts use the most recent 30 observed dates to remain readable.
    funnel_chart = []
    for row in sorted(funnel, key=lambda r: r.get("event_date", ""))[-30:]:
        funnel_chart.append({
            "label": row.get("event_date", ""),
            "view": number(row.get("view")),
            "add_to_cart": number(row.get("add_to_cart")),
            "purchase": number(row.get("purchase")),
        })

    by_day: dict[str, dict[str, float]] = {}
    devices = sorted({row.get("device", "unknown") for row in abandoned})
    for row in abandoned:
        day = row.get("day", "")
        device = row.get("device", "unknown")
        by_day.setdefault(day, {name: 0.0 for name in devices})[device] = number(row.get("abandoned_sessions"))
    abandonment_chart = [
        {"label": day, **values}
        for day, values in sorted(by_day.items())[-30:]
    ]

    retention_columns = []
    if retention:
        retention_columns = [key for key in retention[0] if key != "cohort"]
    retention_rows = []
    for row in sorted(retention, key=lambda r: r.get("cohort", "")):
        cells = []
        for column in retention_columns:
            value = number(row.get(column), default=-1)
            if value < 0:
                cells.append("<td class=\"empty\">—</td>")
            else:
                alpha = max(0.08, min(1.0, value))
                cells.append(
                    f'<td style="--retention:{alpha:.4f}" title="{html.escape(column)}: {value:.1%}">{value:.0%}</td>'
                )
        retention_rows.append(
            f"<tr><th>{html.escape(row.get('cohort', ''))}</th>{''.join(cells)}</tr>"
        )

    seg_summary: dict[str, dict[str, float]] = {}
    for row in segments:
        name = row.get("segment") or "Unassigned"
        agg = seg_summary.setdefault(name, {"customers": 0.0, "ltv": 0.0, "propensity": 0.0, "abandon_rate": 0.0})
        agg["customers"] += 1
        agg["ltv"] += number(row.get("ltv"))
        agg["propensity"] += number(row.get("propensity"))
        agg["abandon_rate"] += number(row.get("abandon_rate"))
    segment_rows = []
    for name, values in sorted(seg_summary.items(), key=lambda item: item[1]["customers"], reverse=True):
        n = max(1.0, values["customers"])
        segment_rows.append(
            "<tr>"
            f"<td>{html.escape(name)}</td><td>{fmt_int(values['customers'])}</td>"
            f"<td>{fmt_money(values['ltv'] / n)}</td>"
            f"<td>{values['propensity'] / n:.1%}</td>"
            f"<td>{values['abandon_rate'] / n:.1%}</td></tr>"
        )

    data_obj = {"funnel": funnel_chart, "abandonment": abandonment_chart, "devices": devices}
    embedded_json = json.dumps(data_obj, ensure_ascii=False).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    retention_header = "".join(f"<th>{html.escape(c)}</th>" for c in retention_columns)
    retention_table = (
        "<table><thead><tr><th>Cohort</th>" + retention_header + "</tr></thead><tbody>"
        + "".join(retention_rows) + "</tbody></table>"
        if retention_rows else '<p class="empty-state">Cohort export is not present in this run.</p>'
    )
    segment_table = (
        "<table><thead><tr><th>Segment</th><th>Customers</th><th>Avg. LTV</th><th>Avg. propensity</th><th>Avg. abandonment</th></tr></thead><tbody>"
        + "".join(segment_rows) + "</tbody></table>"
        if segment_rows else '<p class="empty-state">Segment export is not present in this run.</p>'
    )

    html_doc = f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Customer360 Analytics</title>
<style>
:root{{--bg:#0b1220;--panel:#121d30;--panel2:#18263b;--text:#e8eef8;--muted:#9aabc2;--teal:#48d6bf;--blue:#6ca8ff;--gold:#f3bf68;--pink:#f58ba6;--line:#2b3a50}}
*{{box-sizing:border-box}}body{{margin:0;background:radial-gradient(circle at 10% 0%,#1a2c46 0,#0b1220 46%);color:var(--text);font:14px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}}
.wrap{{max-width:1440px;margin:auto;padding:28px}}header{{display:flex;justify-content:space-between;align-items:center;gap:20px;margin-bottom:26px}}.brand{{display:flex;gap:14px;align-items:center}}.mark{{width:48px;height:48px;border-radius:15px;background:linear-gradient(135deg,var(--teal),var(--blue));display:grid;place-items:center;color:#071521;font-size:24px;font-weight:900}}h1{{font-size:27px;letter-spacing:-.6px;margin:0}}.sub{{color:var(--muted);margin-top:4px}}.status{{border:1px solid #28695f;background:#153b3b;color:#a7fff0;padding:8px 12px;border-radius:999px;font-weight:700;font-size:12px}}
nav{{display:flex;gap:8px;flex-wrap:wrap;margin:0 0 20px}}nav button{{border:1px solid var(--line);border-radius:11px;padding:10px 15px;background:var(--panel);color:var(--muted);cursor:pointer;font-weight:700}}nav button.active,nav button:hover{{background:#173a44;border-color:#2b8d82;color:#d5fff7}}.page{{display:none}}.page.active{{display:block}}.grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-bottom:18px}}.card{{background:linear-gradient(155deg,var(--panel2),var(--panel));border:1px solid var(--line);border-radius:18px;padding:18px;box-shadow:0 10px 28px #0002}}.label{{font-size:12px;color:var(--muted);font-weight:650;text-transform:uppercase;letter-spacing:.8px}}.value{{font-size:28px;font-weight:800;letter-spacing:-.7px;margin-top:9px}}.hint{{font-size:12px;color:var(--muted);margin-top:5px}}.split{{display:grid;grid-template-columns:1.7fr 1fr;gap:16px}}h2{{font-size:16px;margin:0 0 6px}}.panel-note{{color:var(--muted);font-size:12px;margin-bottom:15px}}canvas{{width:100%;height:290px;display:block}}.funnel-list{{display:grid;gap:12px}}.funnel-row{{display:grid;grid-template-columns:120px 1fr 60px;align-items:center;gap:10px;font-size:12px}}.track{{height:10px;border-radius:20px;background:#26374e;overflow:hidden}}.fill{{height:100%;border-radius:20px;background:linear-gradient(90deg,var(--teal),var(--blue));width:var(--width)}}.fill.gold{{background:linear-gradient(90deg,var(--gold),#f58ba6)}}.table-wrap{{overflow:auto;max-width:100%;border:1px solid var(--line);border-radius:12px}}table{{border-collapse:collapse;white-space:nowrap;width:100%;font-size:12px}}th,td{{padding:9px 11px;text-align:right;border-bottom:1px solid #28374c}}th{{background:#18263b;color:#b9cbe0;font-weight:700}}th:first-child,td:first-child{{text-align:left}}tbody tr:hover{{background:#20304755}}.empty{{color:#61728b}}.retention td{{background:rgba(72,214,191,var(--retention,.05));min-width:55px}}.note{{margin-top:16px;color:var(--muted);font-size:12px;line-height:1.7}}.pill{{display:inline-block;padding:4px 8px;border-radius:999px;background:#263a4f;color:#bed2e9;font-size:11px}}.feed-status{{border:1px dashed #48627d;padding:14px;border-radius:13px;color:var(--muted);background:#0d1727}}footer{{border-top:1px solid var(--line);margin-top:30px;padding-top:16px;color:#71839b;font-size:11px}}
@media(max-width:900px){{.grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}.split{{grid-template-columns:1fr}}.wrap{{padding:18px}}header{{align-items:flex-start;flex-direction:column}}}}@media(max-width:480px){{.grid{{grid-template-columns:1fr 1fr}}.value{{font-size:22px}}h1{{font-size:23px}}}}
</style></head><body><div class="wrap">
<header><div class="brand"><div class="mark">C</div><div><h1>Customer360 Analytics</h1><div class="sub">Customer intelligence · sales funnel · retention · abandoned carts</div></div></div><div class="status">OFFLINE DASHBOARD · CSV SNAPSHOT</div></header>
<nav><button class="active" data-page="sales">01 · Sales funnel</button><button data-page="cohort">02 · Cohort retention</button><button data-page="abandoned">03 · Abandoned carts & segments</button></nav>
<section id="sales" class="page active">
<div class="grid">
<div class="card"><div class="label">View sessions</div><div class="value">{fmt_int(views)}</div><div class="hint">Unique sessions by day, summed</div></div>
<div class="card"><div class="label">Cart sessions</div><div class="value">{fmt_int(carts)}</div><div class="hint">Cart rate {cart_rate:.1%} of view sessions</div></div>
<div class="card"><div class="label">Purchase sessions</div><div class="value">{fmt_int(purchases)}</div><div class="hint">Conversion {conversion_rate:.1%} of view sessions</div></div>
<div class="card"><div class="label">Order revenue</div><div class="value">{fmt_money(revenue)}</div><div class="hint">{fmt_int(len(orders))} order rows in the local snapshot</div></div></div>
<div class="split"><div class="card"><h2>Daily session volume</h2><div class="panel-note">Last 30 available days · sessions by event type</div><canvas id="funnelChart" aria-label="Daily sales funnel chart"></canvas></div>
<div class="card"><h2>Funnel conversion</h2><div class="panel-note">Aggregated values from the exported daily session counts</div><div class="funnel-list">
<div class="funnel-row"><span>Views</span><div class="track"><div class="fill" style="--width:100%"></div></div><b>{fmt_int(views)}</b></div>
<div class="funnel-row"><span>Add to cart</span><div class="track"><div class="fill" style="--width:{(carts / views * 100 if views else 0):.2f}%"></div></div><b>{fmt_int(carts)}</b></div>
<div class="funnel-row"><span>Checkout</span><div class="track"><div class="fill gold" style="--width:{(checkouts / views * 100 if views else 0):.2f}%"></div></div><b>{fmt_int(checkouts)}</b></div>
<div class="funnel-row"><span>Purchase</span><div class="track"><div class="fill gold" style="--width:{(purchases / views * 100 if views else 0):.2f}%"></div></div><b>{fmt_int(purchases)}</b></div></div>
<div class="note">The funnel sums daily session counts; a session present on multiple dates may be counted on each date. Use the warehouse measures for a globally distinct session KPI.</div></div></div></section>
<section id="cohort" class="page"><div class="grid"><div class="card"><div class="label">Customer cohorts</div><div class="value">{fmt_int(len(retention))}</div><div class="hint">Monthly first-order cohorts</div></div><div class="card"><div class="label">Retention columns</div><div class="value">{fmt_int(len(retention_columns))}</div><div class="hint">Months since first order</div></div><div class="card"><div class="label">Avg. first-month retention</div><div class="value">{(mean([number(row.get('1')) for row in retention if row.get('1') not in (None, '')]) if any(row.get('1') not in (None, '') for row in retention) else 0):.1%}</div><div class="hint">Available cohort rows only</div></div><div class="card"><div class="label">Interpretation</div><div class="value">Cohort</div><div class="hint">Index 0 is the starting month</div></div></div>
<div class="card"><h2>Retention matrix</h2><div class="panel-note">Cell values are cohort retention rates; teal intensity increases with retention.</div><div class="table-wrap retention">{retention_table}</div><div class="note">Retention compares customers with an order in each cohort month to that cohort's starting population. Later columns can be blank when the cohort has not yet matured.</div></div></section>
<section id="abandoned" class="page"><div class="grid"><div class="card"><div class="label">Abandoned cart sessions</div><div class="value">{fmt_int(abandoned_count)}</div><div class="hint">From offline page-3 extract</div></div><div class="card"><div class="label">Customers scored</div><div class="value">{fmt_int(customer_count)}</div><div class="hint">Customer segment export rows</div></div><div class="card"><div class="label">Average propensity</div><div class="value">{avg_propensity:.1%}</div><div class="hint">Model score, not measured conversion rate</div></div><div class="card"><div class="label">Mean customer LTV</div><div class="value">{fmt_money(avg_ltv)}</div><div class="hint">Across current customer export rows</div></div></div>
<div class="split"><div class="card"><h2>Abandoned sessions by device</h2><div class="panel-note">Last 30 observed dates, stacked by device</div><canvas id="abandonedChart" aria-label="Abandoned cart chart"></canvas></div><div class="card"><h2>Segment overview</h2><div class="panel-note">Average values derived from the customer scoring export</div><div class="table-wrap">{segment_table}</div><div class="note">Propensity is a model estimate, not an observed purchase probability guarantee. Recovery-rate and LTV-lift claims require a controlled experiment.</div></div></div>
<div class="card" style="margin-top:16px"><h2>Live brand sentiment</h2><div class="feed-status"><span class="pill">External streaming feed</span><p>Live sentiment is intentionally not fabricated in this offline snapshot. Start the optional live-feeds profile and the feed landing/Databricks jobs to populate the one-minute sentiment metric from real ingested mentions.</p></div></div></section>
<footer>Customer360 local snapshot · Generated from BI CSV exports · No data is transmitted to a third party · Not a Power BI .pbix file</footer></div>
<script>
const C360 = {embedded_json};
const palette={{view:'#6ca8ff',add_to_cart:'#48d6bf',purchase:'#f3bf68'}};
function drawGrouped(canvasId, rows, series){{
 const canvas=document.getElementById(canvasId); if(!canvas)return; const rect=canvas.getBoundingClientRect(); const dpr=window.devicePixelRatio||1; canvas.width=Math.max(320,rect.width)*dpr; canvas.height=290*dpr; const ctx=canvas.getContext('2d'); ctx.scale(dpr,dpr); const w=canvas.width/dpr,h=canvas.height/dpr; const L=44,R=12,T=16,B=56,plotW=w-L-R,plotH=h-T-B; ctx.clearRect(0,0,w,h);
 const maxVal=Math.max(1,...rows.flatMap(r=>series.map(s=>Number(r[s.key]||0)))); const nice=Math.ceil(maxVal/4); ctx.font='11px system-ui'; ctx.textAlign='right'; ctx.textBaseline='middle';
 for(let i=0;i<=4;i++){{const val=nice*i,y=T+plotH-(val/maxVal)*plotH;ctx.strokeStyle='#2b3a50';ctx.beginPath();ctx.moveTo(L,y);ctx.lineTo(w-R,y);ctx.stroke();ctx.fillStyle='#9aabc2';ctx.fillText(val.toLocaleString(),L-7,y);}}
 const groupW=plotW/Math.max(1,rows.length),barW=Math.max(1,groupW*.72/series.length);
 rows.forEach((r,i)=>{{series.forEach((s,j)=>{{const val=Number(r[s.key]||0),x=L+i*groupW+(groupW-series.length*barW)/2+j*barW, bh=(val/maxVal)*plotH;ctx.fillStyle=s.color;ctx.fillRect(x,T+plotH-bh,Math.max(1,barW-1),bh);}});if(i%Math.max(1,Math.ceil(rows.length/8))===0||i===rows.length-1){{ctx.save();ctx.translate(L+i*groupW+groupW/2,T+plotH+8);ctx.rotate(-.55);ctx.fillStyle='#9aabc2';ctx.textAlign='right';ctx.textBaseline='middle';ctx.fillText(String(r.label).slice(5),0,0);ctx.restore();}}}});
 const legendY=h-8; let x=L;ctx.font='11px system-ui';ctx.textAlign='left';series.forEach(s=>{{ctx.fillStyle=s.color;ctx.fillRect(x,legendY-7,9,9);ctx.fillStyle='#c7d3e3';ctx.fillText(s.label,x+13,legendY);x+=ctx.measureText(s.label).width+34;}});
}}
function drawStacked(canvasId, rows, names){{
 const series=names.map((key,i)=>({{key,label:key,color:['#48d6bf','#6ca8ff','#f3bf68','#f58ba6'][i%4]}}));drawGrouped(canvasId,rows,series);
}}
drawGrouped('funnelChart',C360.funnel,[{{key:'view',label:'Views',color:palette.view}},{{key:'add_to_cart',label:'Cart',color:palette.add_to_cart}},{{key:'purchase',label:'Purchase',color:palette.purchase}}]);
drawStacked('abandonedChart',C360.abandonment,C360.devices);
document.querySelectorAll('nav button').forEach(btn=>btn.addEventListener('click',()=>{{document.querySelectorAll('nav button').forEach(b=>b.classList.toggle('active',b===btn));document.querySelectorAll('.page').forEach(p=>p.classList.toggle('active',p.id===btn.dataset.page));if(btn.dataset.page==='sales')drawGrouped('funnelChart',C360.funnel,[{{key:'view',label:'Views',color:palette.view}},{{key:'add_to_cart',label:'Cart',color:palette.add_to_cart}},{{key:'purchase',label:'Purchase',color:palette.purchase}}]);if(btn.dataset.page==='abandoned')drawStacked('abandonedChart',C360.abandonment,C360.devices);}}));
window.addEventListener('resize',()=>{{if(document.getElementById('sales').classList.contains('active'))drawGrouped('funnelChart',C360.funnel,[{{key:'view',label:'Views',color:palette.view}},{{key:'add_to_cart',label:'Cart',color:palette.add_to_cart}},{{key:'purchase',label:'Purchase',color:palette.purchase}}]);if(document.getElementById('abandoned').classList.contains('active'))drawStacked('abandonedChart',C360.abandonment,C360.devices);}});
</script></body></html>'''
    output = bi_root / "dashboard.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html_doc, encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_root", nargs="?", default="data", help="Generated local data root")
    args = parser.parse_args()
    result = build_dashboard(Path(args.data_root))
    print(f"CUSTOMER360_DASHBOARD_BUILT={result.resolve()}")


if __name__ == "__main__":
    main()
