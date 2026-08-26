"""Tests for the CDC and telemetry producer logic.

No Snowflake connection required — everything runs through in-process sinks
or the --dry-run path.

How to run:
    cd streaming-cdc-iceberg-vhol/producer
    pip install -r tests/requirements-dev.txt
    pytest tests/ -v
"""

from __future__ import annotations

import time
from types import SimpleNamespace

import producer as p

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_args(**overrides) -> SimpleNamespace:
    """Minimal args namespace for driving simulators without Snowflake."""
    defaults = {
        "seed": 42,
        "rate": 2.0,
        "update_rate": 0.15,
        "delete_rate": 0.005,
        "reinspect": False,
        "incident": False,
        "incident_after": 90.0,
        "incident_minutes": 20.0,
        "cdc_mode": "journal",
        "dry_run": True,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class CollectingSink(p.CdcSink):
    """In-memory sink — records every event for assertion."""

    def __init__(self):
        self.inserts: list = []  # [(row, msn, lsn)]
        self.updates: list = []  # [(old_key, new_row, msn, lsn)]
        self.deletes: list = []  # [(key, msn, lsn)]

    def emit_insert(self, row, msn, lsn):
        self.inserts.append((row, msn, lsn))

    def emit_update(self, old_key, new_row, msn, lsn):
        self.updates.append((old_key, new_row, msn, lsn))

    def emit_delete(self, key, msn, lsn):
        self.deletes.append((key, msn, lsn))


# ---------------------------------------------------------------------------
# journal_event wire shape
# ---------------------------------------------------------------------------


class TestJournalEventShape:
    def test_insert_fills_all_payload_columns(self):
        row = {c: f"v-{c}" for c in p.SOURCE_COLUMNS}
        row["EVENT_TS"] = "2024-01-01T00:00:00.000"
        row["UPDATED_TS"] = "2024-01-01T00:00:00.000"
        ev = p.journal_event("S-abc", p.EV_INSERT, row, 10000, 10001)
        assert ev["EVENT_TYPE"] == p.EV_INSERT
        assert ev["PRIMARY_KEY__SCAN_ID"] == "S-abc"
        for col in p.SOURCE_COLUMNS:
            assert f"PAYLOAD__{col}" in ev, f"PAYLOAD__{col} missing from INSERT event"

    def test_update_carries_old_key_and_new_payload(self):
        """PRIMARY_KEY__* holds the old key; PAYLOAD__* holds the new values."""
        row = {c: f"new-{c}" for c in p.SOURCE_COLUMNS}
        row["EVENT_TS"] = "2024-01-01T00:00:00.000"
        row["UPDATED_TS"] = "2024-01-01T00:00:00.000"
        ev = p.journal_event("OLD-KEY", p.EV_UPDATE, row, 20000, 20001)
        assert ev["EVENT_TYPE"] == p.EV_UPDATE
        assert ev["PRIMARY_KEY__SCAN_ID"] == "OLD-KEY"
        assert ev["PAYLOAD__SCAN_ID"] == "new-SCAN_ID"

    def test_delete_has_null_payload_and_correct_key(self):
        """On DELETE the connector sends the key only; every PAYLOAD__* is NULL."""
        ev = p.journal_event("S-xyz", p.EV_DELETE, None, 30000, 30001)
        assert ev["EVENT_TYPE"] == p.EV_DELETE
        assert ev["PRIMARY_KEY__SCAN_ID"] == "S-xyz"
        for col in p.SOURCE_COLUMNS:
            assert ev[f"PAYLOAD__{col}"] is None, (
                f"PAYLOAD__{col} must be NULL on DELETE — MERGE branches on this"
            )


# ---------------------------------------------------------------------------
# Soft-delete: the flag is always present and never dropped
# ---------------------------------------------------------------------------


class TestSoftDelete:
    def test_delete_event_payload_all_null(self):
        """Soft-delete wire shape: key present, every payload column NULL."""
        ev = p.journal_event("S-del", p.EV_DELETE, None, 1000, 1001)
        for col in p.SOURCE_COLUMNS:
            assert ev[f"PAYLOAD__{col}"] is None

    def test_simulator_emits_delete_not_physical_remove(self):
        """Voided scan → DELETE journal event; row key is still traceable."""
        sink = CollectingSink()
        # delete_rate=1.0 forces a void for every insert after the list warms up
        sim = p.CdcSimulator(make_args(seed=1, update_rate=0.0, delete_rate=1.0), sink)
        for _ in range(10):
            sim.tick(5)
        assert len(sink.deletes) > 0, "expected soft-delete events at delete_rate=1.0"
        for key, _msn, _lsn in sink.deletes:
            assert isinstance(key, str) and key.startswith("S-"), (
                f"delete event must carry a scan ID string, got {key!r}"
            )


# ---------------------------------------------------------------------------
# Incident shaping: humidity precedes the defect spike
# ---------------------------------------------------------------------------


class TestIncidentShaping:
    def test_defect_rate_at_baseline_before_incident(self):
        sim = p.CdcSimulator(make_args(seed=99), CollectingSink())
        assert sim.defect_rate("PAINT") == p.BASE_DEFECT_RATE["PAINT"]
        assert not sim.incident_active()

    def test_incident_elevates_only_paint(self):
        """Only PAINT defect rate changes; WELD and ASSEMBLY stay at baseline."""
        sim = p.CdcSimulator(make_args(seed=7), CollectingSink())
        sim.start_incident(minutes=60)
        assert sim.incident_active()
        assert sim.defect_rate("PAINT") == p.INCIDENT_DEFECT_RATE["PAINT"]
        assert sim.defect_rate("WELD") == p.BASE_DEFECT_RATE["WELD"]
        assert sim.defect_rate("ASSEMBLY") == p.BASE_DEFECT_RATE["ASSEMBLY"]

    def test_humidity_starts_at_baseline_then_ramps(self):
        """current_humidity() is at baseline before drift, then climbs toward INCIDENT_HUMIDITY."""
        args = make_args(seed=10)
        telem = p.TelemetrySimulator(args, p.DryRunTelemetrySink())
        baseline = p.METRICS["booth_humidity"][1]
        assert telem.current_humidity() == baseline

        telem.start_drift(ramp_seconds=0.001)  # near-instant ramp for test speed
        time.sleep(0.02)
        h = telem.current_humidity()
        assert h > baseline, "humidity should rise above baseline after drift starts"
        assert h <= p.INCIDENT_HUMIDITY + 0.1, "humidity must not exceed incident target"

    def test_cdc_defect_rate_unaffected_while_humidity_drifts(self):
        """Humidity drift and CDC defect rate are decoupled — humidity leads by design."""
        args = make_args(seed=99, incident=True)
        telem = p.TelemetrySimulator(args, p.DryRunTelemetrySink())
        cdc = p.CdcSimulator(args, CollectingSink())
        telem.start_drift(90.0)  # arm humidity drift as main() does for --incident
        # CDC defect rate is still baseline even though humidity is drifting
        assert cdc.defect_rate("PAINT") == p.BASE_DEFECT_RATE["PAINT"]


# ---------------------------------------------------------------------------
# Reinspect / recovery: failed frames flip to PASS, aggregates must decrease
# ---------------------------------------------------------------------------


class TestReinspectShaping:
    def test_reinspect_emits_update_events(self):
        sink = CollectingSink()
        sim = p.CdcSimulator(make_args(seed=5, reinspect=True), sink)
        for _ in range(20):
            sim.tick(5)
        assert len(sink.updates) > 0, "reinspect mode must emit UPDATE events"

    def test_overturned_frame_becomes_pass(self):
        """Every UPDATE in reinspect mode must set STATUS=PASS and clear DEFECT_CODE."""
        sink = CollectingSink()
        sim = p.CdcSimulator(make_args(seed=6, reinspect=True), sink)
        for _ in range(20):
            sim.tick(5)
        for _old_key, new_row, _msn, _lsn in sink.updates:
            assert new_row["STATUS"] == "PASS", "overturned row must be PASS"
            assert new_row["DEFECT_CODE"] is None, "overturned row must have no defect code"

    def test_update_key_refers_to_an_inserted_scan(self):
        """The UPDATE old_key must reference a previously inserted scan ID."""
        sink = CollectingSink()
        sim = p.CdcSimulator(make_args(seed=5, reinspect=True, delete_rate=0.0), sink)
        for _ in range(20):
            sim.tick(5)
        inserted_ids = {r["SCAN_ID"] for r, _, _ in sink.inserts}
        for old_key, _new_row, _msn, _lsn in sink.updates:
            assert old_key in inserted_ids, (
                f"UPDATE key {old_key!r} was never inserted in this run"
            )


# ---------------------------------------------------------------------------
# Rate arithmetic
# ---------------------------------------------------------------------------


class TestRateArithmetic:
    def test_tick_n_produces_exactly_n_inserts(self):
        sink = CollectingSink()
        sim = p.CdcSimulator(make_args(seed=42, update_rate=0.0, delete_rate=0.0), sink)
        sim.tick(100)
        assert len(sink.inserts) == 100

    def test_delete_rate_one_produces_roughly_one_delete_per_insert(self):
        sink = CollectingSink()
        sim = p.CdcSimulator(make_args(seed=42, update_rate=0.0, delete_rate=1.0), sink)
        for _ in range(10):
            sim.tick(10)
        n_del = sim.counts["delete"]
        # Poisson rounding means not exactly 100, but well within a generous window
        assert 70 <= n_del <= 130, f"expected ~100 deletes at delete_rate=1.0, got {n_del}"

    def test_update_rate_nonzero_with_fails_produces_updates(self):
        sink = CollectingSink()
        sim = p.CdcSimulator(make_args(seed=42, update_rate=1.0, delete_rate=0.0), sink)
        for _ in range(50):
            sim.tick(10)
        assert sim.counts["update"] > 0, "update_rate>0 with failures must yield updates"


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def _run(self, seed: int) -> list[tuple]:
        sink = CollectingSink()
        sim = p.CdcSimulator(make_args(seed=seed), sink)
        for _ in range(10):
            sim.tick(5)
        # SCAN_ID uses uuid4() which is not seeded by self.rng — exclude it.
        # All other fields (LINE, STATUS, DEFECT_CODE, SKU, FRAME_ID) are RNG-seeded.
        return [
            (r["FRAME_ID"], r["STATUS"], r["LINE"], r["DEFECT_CODE"])
            for r, _, _ in sink.inserts
        ]

    def test_same_seed_produces_identical_output(self):
        assert self._run(1234) == self._run(1234)

    def test_different_seeds_produce_different_output(self):
        assert self._run(1) != self._run(2)
