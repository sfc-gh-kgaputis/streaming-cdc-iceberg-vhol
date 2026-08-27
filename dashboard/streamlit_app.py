"""Plant Floor — Live Quality Dashboard (Streamlit in Snowflake).

Renders yield and booth humidity on a shared time axis, so the humidity spike
is visible ahead of the PAINT yield drop during Part 5's incident.

Requires: Streamlit in Snowflake (SiS) — uses get_active_session().
Auto-refreshes every 30 s, in place, via st.fragment(run_every=...). An Auto Refresh
toggle and a Refresh button sit above the heading; turn auto off when you want the
chart to hold still while you talk over it.
Runs on a warehouse runtime: no compute pool. environment.yml pins Streamlit, and the
pin is load-bearing — a warehouse runtime does not resolve the newest version it
supports, and this app needs st.fragment (1.37+) and horizontal containers (1.49+).
Reads MFG.ANALYTICS.YIELD_BY_LINE_5MIN and DEFECT_COUNTS_5MIN by column name,
so run the Part 3 column contract before deploying it.
"""

from __future__ import annotations

import datetime

import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REFRESH_SECS = 30
LOOKBACK_MINUTES = 60

_LINE_DOMAIN = ["PAINT", "WELD", "ASSEMBLY"]
_LINE_RANGE = ["#E8500A", "#29B5E8", "#2ECC71"]   # orange, sky, green
_HUMIDITY_COLOR = "#FF9800"


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

def _fetch_yield(session) -> pd.DataFrame:
    df = session.sql(f"""
        SELECT
            LINE,
            BUCKET,
            UNITS,
            SCRAP_UNITS,
            -- Keep this cast. The Dynamic Table now declares NUMBER(5,2) rather than the
            -- inferred NUMBER(29,2), which fixes the Iceberg schema an external engine
            -- reads -- but it does not change the Python connector, which maps ANY
            -- NUMBER with scale > 0 to decimal.Decimal. Altair cannot plot a Decimal.
            FIRST_PASS_YIELD_PCT::FLOAT AS FIRST_PASS_YIELD_PCT,
            AVG_BOOTH_HUMIDITY
        FROM MFG.ANALYTICS.YIELD_BY_LINE_5MIN
        WHERE BUCKET >= DATEADD('minute', -{LOOKBACK_MINUTES}, CURRENT_TIMESTAMP())
        ORDER BY BUCKET ASC, LINE
    """).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df


def _fetch_defects(session) -> pd.DataFrame:
    df = session.sql("""
        SELECT LINE, DEFECT_CODE, SUM(N) AS N
        FROM MFG.ANALYTICS.DEFECT_COUNTS_5MIN
        WHERE DEFECT_CODE <> 'NONE'
          AND BUCKET >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
        GROUP BY 1, 2
        ORDER BY N DESC
    """).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def _yield_humidity_chart(yield_df: pd.DataFrame) -> alt.LayerChart | alt.Chart:
    """Yield % per line (left axis) + PAINT booth humidity (right, dashed)."""
    color_scale = alt.Scale(domain=_LINE_DOMAIN, range=_LINE_RANGE)
    x_enc = alt.X("bucket:T", title="Time (5-min buckets)",
                  axis=alt.Axis(format="%H:%M"))

    yield_chart = (
        alt.Chart(yield_df[["line", "bucket", "first_pass_yield_pct"]])
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=x_enc,
            y=alt.Y(
                "first_pass_yield_pct:Q",
                title="First-Pass Yield %",
                scale=alt.Scale(domain=[60, 102]),
            ),
            color=alt.Color("line:N", scale=color_scale,
                            legend=alt.Legend(title="Line")),
            tooltip=[
                alt.Tooltip("line:N", title="Line"),
                alt.Tooltip("bucket:T", title="Bucket", format="%H:%M"),
                alt.Tooltip("first_pass_yield_pct:Q", title="Yield %", format=".1f"),
            ],
        )
        .properties(height=340)
    )

    # Humidity is only present for PAINT — NULL for WELD and ASSEMBLY by design.
    hum = yield_df[yield_df["avg_booth_humidity"].notna()][
        ["bucket", "avg_booth_humidity"]
    ]
    if hum.empty:
        return yield_chart.properties(
            title=alt.TitleParams("First-Pass Yield — all lines")
        )

    humidity_chart = (
        alt.Chart(hum)
        .mark_line(strokeDash=[5, 3], strokeWidth=2,
                   color=_HUMIDITY_COLOR, point=True)
        .encode(
            x=x_enc,
            y=alt.Y(
                "avg_booth_humidity:Q",
                title="Paint-Booth Humidity (%)",
                scale=alt.Scale(domain=[0, 100]),
                axis=alt.Axis(titleColor=_HUMIDITY_COLOR),
            ),
            tooltip=[
                alt.Tooltip("bucket:T", title="Bucket", format="%H:%M"),
                alt.Tooltip("avg_booth_humidity:Q",
                            title="Humidity %", format=".1f"),
            ],
        )
        .properties(height=340)
    )

    return (
        alt.layer(yield_chart, humidity_chart)
        .resolve_scale(y="independent")
        .properties(
            title=alt.TitleParams(
                "First-Pass Yield vs Paint-Booth Humidity",
                subtitle=[
                    "WELD and ASSEMBLY are the control — humidity is a paint-booth metric only.",
                    "Humidity (dashed orange) rises before PAINT yield falls.",
                ],
            )
        )
    )


def _defect_chart(defect_df: pd.DataFrame) -> alt.Chart:
    color_scale = alt.Scale(domain=_LINE_DOMAIN, range=_LINE_RANGE)
    return (
        alt.Chart(defect_df)
        .mark_bar()
        .encode(
            x=alt.X("n:Q", title="Count (last 15 min)"),
            y=alt.Y("defect_code:N", sort="-x", title="Defect Code"),
            color=alt.Color("line:N", scale=color_scale,
                            legend=alt.Legend(title="Line")),
            tooltip=[
                alt.Tooltip("line:N", title="Line"),
                alt.Tooltip("defect_code:N", title="Defect Code"),
                alt.Tooltip("n:Q", title="Count"),
            ],
        )
        .properties(height=220)
    )


# ---------------------------------------------------------------------------
# KPI tiles
# ---------------------------------------------------------------------------

def _kpi_tiles(yield_df: pd.DataFrame) -> None:
    latest_bucket = yield_df["bucket"].max()
    latest = (
        yield_df[yield_df["bucket"] == latest_bucket]
        .set_index("line")
    )
    cols = st.columns(3)
    for col, line in zip(cols, _LINE_DOMAIN):
        if line in latest.index:
            row = latest.loc[line]
            yld = float(row["first_pass_yield_pct"])
            # delta_color="inverse" turns red when value is low (scrap high)
            col.metric(
                label=f"{line}  —  First-Pass Yield",
                value=f"{yld:.1f}%",
                delta=f"{int(row['units'])} units, {int(row['scrap_units'])} scrap",
                delta_color="inverse" if yld < 85 else "normal",
            )
        else:
            col.metric(label=f"{line}  —  First-Pass Yield", value="—")


# ---------------------------------------------------------------------------
# Auto-refreshing fragment
# ---------------------------------------------------------------------------

def live_dashboard() -> None:
    session = get_active_session()

    try:
        yield_df = _fetch_yield(session)
        defect_df = _fetch_defects(session)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Query failed: {exc}")
        return

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC")
    auto_on = st.session_state.get("auto_refresh", True)
    mode = (
        f"auto-refreshing every {REFRESH_SECS} s" if auto_on
        else "auto-refresh disabled"
    )
    st.caption(f"Last refresh: {now}  ·  {mode}  ·  Streamlit in Snowflake")

    if yield_df.empty:
        st.info(
            f"No data yet — no 5-minute bucket has closed in the last {LOOKBACK_MINUTES} "
            "minutes. Either the producer is not running, or the Dynamic Tables are "
            "suspended. Check `SHOW DYNAMIC TABLES IN SCHEMA MFG.ANALYTICS` for a "
            "`scheduling_state` of SUSPENDED, and resume upstream tables first."
        )
        return

    _kpi_tiles(yield_df)

    st.divider()
    st.altair_chart(_yield_humidity_chart(yield_df), use_container_width=True)

    st.divider()
    st.subheader("Defect breakdown — last 15 min")
    if defect_df.empty:
        st.caption("No defects recorded in the last 15 minutes.")
    else:
        st.altair_chart(_defect_chart(defect_df), use_container_width=True)


# ---------------------------------------------------------------------------
# App shell
# ---------------------------------------------------------------------------

# Streamlit in Snowflake ignores page_title, page_icon and menu_items, so the
# window title comes from the STREAMLIT object's TITLE instead.
st.set_page_config(layout="wide")

# A horizontal container right-aligns the controls and sizes them to their content,
# so they sit next to each other and no column split can wrap the labels. The heading
# gets its own full-width row underneath.
#
# Both controls sit OUTSIDE the fragment deliberately. `run_every` is bound when the
# fragment is created, so toggling auto-refresh has to rerun the whole script to take
# effect; a widget inside the fragment would only rerun the fragment. Clicking a button
# reruns the script too, and nothing here is cached, so that click is the manual refresh.
_controls = st.container(
    horizontal=True,
    horizontal_alignment="right",
    vertical_alignment="center",
)
_controls.toggle("Auto Refresh", value=True, key="auto_refresh")
_controls.button("Refresh")

st.title("Plant Floor — Live Quality Dashboard")

_auto_on = st.session_state.get("auto_refresh", True)

# st.fragment refreshes only this function, leaving the heading and controls in place,
# where a whole-page rerun would re-lay-out everything on every tick. run_every=None
# turns the timer off without changing anything else.
st.fragment(run_every=REFRESH_SECS if _auto_on else None)(live_dashboard)()
