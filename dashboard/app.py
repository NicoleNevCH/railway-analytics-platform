"""
Railway Analytics — Streamlit dashboard.

A friendly UI on top of the platform:
  * Pipeline status board (Bronze / Silver / DLQ counts) from /pipeline/status.
  * Punctuality KPIs and charts, read from the consumption API (DuckDB/Iceberg).
  * A "chaos generator" that sends events to the ingestion API and shows the
    accept/reject scoreboard — making the validation gate visible live.

It talks only to the two APIs over HTTP; it holds no business logic itself.
"""

from __future__ import annotations

import os

import httpx
import pandas as pd
import streamlit as st

from simulator.chaos_simulator import build_trip_events  # reuse the real chaos logic

CONSUMPTION_URL = os.getenv("CONSUMPTION_URL", "http://consumption-api:8001")
INGESTION_URL = os.getenv("INGESTION_URL", "http://ingestion-api:8000")

st.set_page_config(page_title="Railway Analytics", page_icon="🚆", layout="wide")


# ---------------------------------------------------------------------------
# Small HTTP helpers
# ---------------------------------------------------------------------------
def api_get(path: str, params: dict | None = None):
    try:
        r = httpx.get(f"{CONSUMPTION_URL}{path}", params=params, timeout=60)
        if r.status_code == 503:
            return {"_not_ready": True}
        r.raise_for_status()
        return r.json()
    except httpx.HTTPError as exc:
        return {"_error": str(exc)}


def api_post_refresh():
    try:
        httpx.post(f"{CONSUMPTION_URL}/refresh", timeout=60)
    except httpx.HTTPError:
        pass


def send_events(trips: int, updates: int, null_rate: float, mess_rate: float, delayed_rate: float):
    """Generate chaos events and POST them to the ingestion API in batches."""
    events: list[dict] = []
    for _ in range(trips):
        events.extend(
            build_trip_events(
                updates_per_trip=updates,
                null_rate=null_rate,
                time_mess_rate=mess_rate,
                delayed_rate=delayed_rate,
            )
        )
    received = accepted = rejected = 0
    with httpx.Client(timeout=60) as client:
        for i in range(0, len(events), 25):
            chunk = [{k: v for k, v in e.items() if k != "_chaos"} for e in events[i : i + 25]]
            resp = client.post(f"{INGESTION_URL}/ingest", json=chunk)
            resp.raise_for_status()
            body = resp.json()
            received += body["received"]
            accepted += body["accepted"]
            rejected += body["rejected"]
    return received, accepted, rejected


# ---------------------------------------------------------------------------
# Sidebar — controls
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Controls")

    if st.button("🔄 Refresh data", use_container_width=True):
        api_post_refresh()
        st.rerun()

    st.divider()
    st.subheader("🌩️ Chaos generator")
    st.caption(
        "Create synthetic train events — including deliberate defects — to watch "
        "the pipeline catch bad data without breaking."
    )
    n_trips = st.slider("Trips to generate", 10, 500, 120, step=10)
    n_updates = st.slider(
        "Updates per trip", 1, 6, 3,
        help="The same trip is sent several times (like live status updates). "
             "The pipeline must collapse them into ONE row — this is what proves "
             "the de-duplication (UPSERT) works.",
    )
    pct_missing = st.slider(
        "Faulty events: missing station ID (%)", 0, 50, 12,
        help="This share of events arrives with NO station ID (a sensor glitch). "
             "They fail validation at the gate and are sent to the error queue "
             "(DLQ) — they never reach your clean data.",
    )
    pct_inverted = st.slider(
        "Faulty events: reversed timestamps (%)", 0, 50, 10,
        help="This share of events has the arrival BEFORE the departure "
             "(impossible). Spark catches these later and quarantines them.",
    )
    pct_late = st.slider(
        "Trains actually running late (%)", 0, 80, 35,
        help="How often a trip is genuinely delayed (more than 5 min). Drives how "
             "many show up as DELAYED in the KPIs.",
    )

    if st.button("Send events ▶", type="primary", use_container_width=True):
        with st.spinner("Sending to ingestion API..."):
            try:
                rec, acc, rej = send_events(
                    n_trips, n_updates,
                    pct_missing / 100, pct_inverted / 100, pct_late / 100,
                )
                st.success(f"Sent {rec} • accepted {acc} • rejected→DLQ {rej}")
                st.info("Now run `make process` (Spark), then click **Refresh data**.")
            except Exception as exc:  # noqa: BLE001
                st.error(f"Failed to send: {exc}")

    st.divider()
    st.caption(f"Consumption: {CONSUMPTION_URL}")
    st.caption(f"Ingestion: {INGESTION_URL}")


# ---------------------------------------------------------------------------
# Main — title + pipeline status
# ---------------------------------------------------------------------------
st.title("🚆 Railway Analytics Platform")
st.caption("Local lakehouse — ingestion gate → Bronze → Spark/Iceberg → DuckDB")

# Fetch status FIRST: it refreshes the engine's view to the latest snapshot,
# so the KPI queries below reflect the most recent Spark run automatically.
status = api_get("/pipeline/status")

st.subheader("Pipeline status")
c1, c2, c3, c4 = st.columns(4)
if status and "_error" not in status:
    c1.metric("Bronze · raw events", status["bronze"]["raw_events"])
    c2.metric("Silver · rows", status["silver"]["rows"] if status["silver"]["ready"] else "—")
    c3.metric("DLQ · schema", status["errors"]["schema_rejected"])
    c4.metric("DLQ · temporal", status["errors"]["temporal_rejected"])
    silver_ready = status["silver"]["ready"]
else:
    st.error(f"Could not reach the consumption API. {status.get('_error', '')}")
    silver_ready = False

st.divider()

# ---------------------------------------------------------------------------
# Main — KPIs and charts (only when the Silver table exists)
# ---------------------------------------------------------------------------
if not silver_ready:
    st.info(
        "**No analytical data yet.** Use the **Chaos generator** in the sidebar to "
        "send events, then run `make process` to push them through Spark, then "
        "click **Refresh data**."
    )
    st.stop()

kpis = api_get("/stats/punctuality")
if kpis and "_not_ready" not in kpis and "_error" not in kpis:
    st.subheader("Punctuality KPIs")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total trips", kpis.get("total_trips", 0))
    k2.metric("On-time %", f'{kpis.get("on_time_pct", 0)}%')
    k3.metric("Delayed", kpis.get("delayed_trips", 0))
    k4.metric("Avg delay (min)", kpis.get("avg_delay_minutes", 0))
    k5.metric("Max delay (min)", kpis.get("max_delay_minutes", 0))

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Delay distribution")
    dist = api_get("/stats/delay-distribution")
    if isinstance(dist, list) and dist:
        ddist = pd.DataFrame(dist).set_index("bucket")
        st.bar_chart(ddist[["trips"]])
        st.caption("How many trips fall in each lateness band (minutes).")
    else:
        st.write("No distribution data.")

with right:
    st.subheader("Avg delay by station (top 12)")
    stations = api_get("/stats/by-station", params={"limit": 12})
    if isinstance(stations, list) and stations:
        sdf = pd.DataFrame(stations)
        label = sdf["station_name"].fillna(sdf["current_station_id"])
        sdf = sdf.assign(station=label).set_index("station")
        st.bar_chart(sdf[["avg_delay_minutes"]].fillna(0))
    else:
        st.write("No station data.")

st.divider()

st.subheader("Worst delays")
min_delay = st.slider("Minimum delay (minutes)", 0, 60, 10)
delayed = api_get("/trips/delayed", params={"min_delay": min_delay, "limit": 100})
if isinstance(delayed, list) and delayed:
    ddf = pd.DataFrame(delayed)[
        ["trip_id", "train_number", "operator", "current_station_name", "delay_minutes", "delay_status"]
    ]
    st.dataframe(ddf, use_container_width=True, hide_index=True)
    st.caption(f"{len(ddf)} trips delayed more than {min_delay} min.")
else:
    st.write("No trips above that threshold.")
