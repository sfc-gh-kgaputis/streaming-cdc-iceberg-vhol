"""Plant Floor — Live Quality Dashboard (Streamlit in Snowflake).

Presenter-run demo for Part 5 of the Iceberg CDC VHOL.
Renders yield and booth humidity on a shared time axis so the audience
sees the humidity spike precede the PAINT yield drop.

Requires: Streamlit in Snowflake (SiS) — uses get_active_session().
Auto-refreshes every 30 s via @st.fragment(run_every=...).
All packages are pre-installed in the SiS container; no pyproject.toml needed.
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
            FIRST_PASS_YIELD_PCT::FLOAT AS FIRST_PASS_YIELD_PCT,
            AVG_BOOTH_HUMIDITY
        FROM MFG.CDC.DT_YIELD_BY_LINE_5MIN
        WHERE BUCKET >= DATEADD('minute', -{LOOKBACK_MINUTES}, CURRENT_TIMESTAMP())
        ORDER BY BUCKET ASC, LINE
    """).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df


def _fetch_defects(session) -> pd.DataFrame:
    df = session.sql("""
        SELECT LINE, DEFECT_CODE, SUM(N) AS N
        FROM MFG.CDC.DT_DEFECT_COUNTS_5MIN
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

@st.fragment(run_every=REFRESH_SECS)
def live_dashboard() -> None:
    session = get_active_session()

    try:
        yield_df = _fetch_yield(session)
        defect_df = _fetch_defects(session)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Query failed: {exc}")
        return

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC")
    st.caption(f"Last refresh: {now}  ·  auto-refreshing every {REFRESH_SECS} s")

    if yield_df.empty:
        st.info(
            "No data yet — the pipeline has not produced any 5-minute buckets. "
            "Start the producer and wait ~1 minute for the first bucket."
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

st.set_page_config(page_title="Plant Floor — Live Quality", layout="wide")
st.title("Plant Floor — Live Quality Dashboard")
live_dashboard()
