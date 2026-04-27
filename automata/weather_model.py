"""
Self-tuning forecast-bias model for the weather bot.

Concept
-------
Raw forecasts (Open-Meteo, NOAA) have a station-specific systematic bias that
is roughly additive — London City chronically verifies ~1.2 C cooler than
Open-Meteo says, regardless of whether the forecast is 18 C or 28 C. So we
model:

    actual_max ~ Normal(forecast + bias[icao, source],
                        sigma[icao, lead_bucket])

    fair_no_prob = Phi((threshold - (forecast + bias)) / sigma)

`bias` and `sigma` are learned online via exponential moving average over the
residuals of past outcomes. New outcome → `update_from_outcome()` recomputes
bias + sigma for that station; next scan picks up the updated numbers
automatically. No manual retraining step.

Primary ground truth for `actual_max` is Polymarket's own resolution: for a
temperature-bucket event, the market whose YES token settles at 1.00 tells us
which range the day's high fell into, and we use the bucket midpoint. METAR
history is used as a cross-check / fallback when the bucket isn't available.

Storage: DuckDB at `db/weather_model.db` (analytics-friendly; separate from
SQLite `bets.db` which is the operational store).
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb

log = logging.getLogger("automata.weather_model")

_REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = _REPO_ROOT / "db" / "weather_model.db"
DB_PATH.parent.mkdir(exist_ok=True)


# ── Hyperparameters ──────────────────────────────────────────────────────────

# EMA weight on each new residual. Effective half-life ~ log(0.5)/log(1-ALPHA)
# ≈ 6.6 observations at α=0.10 — fast enough to track regime drift, slow
# enough that a single outlier doesn't jerk the bias around.
ALPHA_BIAS = 0.10
ALPHA_MSE = 0.10

# Warm-start: for the first few observations per (icao, source), weight each
# sample by 1/n instead of ALPHA so early estimates aren't stuck at 0.
WARMUP_SAMPLES = 10

# Fallback priors when we haven't seen enough data for a station yet.
# Units are whatever the market uses; the model keeps everything per-station so
# as long as a given station is consistently °C or consistently °F the priors
# in the table get overwritten within ~WARMUP_SAMPLES.
DEFAULT_BIAS = 0.0
DEFAULT_SIGMA_BY_LEAD = {
    "sub6h":   1.5,   # e.g. 1.5 °C or 1.5 °F — whichever the station uses
    "6-12h":   2.0,
    "12-24h":  2.5,
    "24-48h":  3.0,
    "48h+":    4.0,
}


def lead_bucket(hours: float | None) -> str:
    if hours is None:
        return "24-48h"
    if hours < 6:     return "sub6h"
    if hours < 12:    return "6-12h"
    if hours < 24:    return "12-24h"
    if hours < 48:    return "24-48h"
    return "48h+"


# ── DB schema ─────────────────────────────────────────────────────────────────

def _connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=read_only)


def init_db() -> None:
    """Create tables + sequences if absent. Idempotent, safe to call every startup."""
    con = _connect()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS scans (
                id                BIGINT,
                scanned_at        TIMESTAMPTZ,
                city              VARCHAR,
                icao              VARCHAR,
                event_date        VARCHAR,
                event_slug        VARCHAR,
                question          VARCHAR,
                token_id          VARCHAR,
                yes_token_id      VARCHAR,
                resolution_dt     TIMESTAMPTZ,
                hours_to_res      DOUBLE,
                lead_bucket       VARCHAR,
                unit              VARCHAR,
                threshold         DOUBLE,
                threshold_hi      DOUBLE,
                direction         VARCHAR,
                openmeteo_high    DOUBLE,
                noaa_high         DOUBLE,
                taf_high          DOUBLE,
                metar_current     DOUBLE,
                metar_max_so_far  DOUBLE,
                no_bid            DOUBLE,
                no_ask            DOUBLE,
                yes_bid           DOUBLE,
                yes_ask           DOUBLE,
                calibrated_mu     DOUBLE,
                calibrated_sigma  DOUBLE,
                fair_no_prob      DOUBLE,
                edge_bps          DOUBLE,
                bias_used         DOUBLE,
                source_used       VARCHAR
            )
        """)
        # Migration for DBs created before taf_high existed. DuckDB >= 0.9
        # supports IF NOT EXISTS on ADD COLUMN.
        try:
            con.execute("ALTER TABLE scans ADD COLUMN IF NOT EXISTS taf_high DOUBLE")
        except Exception:
            # Very old DuckDB falls back to a probe-then-add.
            cols = {r[1] for r in con.execute("PRAGMA table_info('scans')").fetchall()}
            if "taf_high" not in cols:
                con.execute("ALTER TABLE scans ADD COLUMN taf_high DOUBLE")
        con.execute("CREATE SEQUENCE IF NOT EXISTS seq_scan START 1")
        con.execute("""
            CREATE TABLE IF NOT EXISTS outcomes (
                icao              VARCHAR,
                event_date        VARCHAR,
                city              VARCHAR,
                unit              VARCHAR,
                actual_high       DOUBLE,
                bucket_lo         DOUBLE,
                bucket_hi         DOUBLE,
                source            VARCHAR,
                event_slug        VARCHAR,
                resolved_at       TIMESTAMPTZ,
                PRIMARY KEY (icao, event_date)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS bias (
                icao          VARCHAR,
                source        VARCHAR,
                bias          DOUBLE,
                n             INTEGER,
                last_updated  TIMESTAMPTZ,
                PRIMARY KEY (icao, source)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS sigma (
                icao          VARCHAR,
                lead_bucket   VARCHAR,
                mse           DOUBLE,
                n             INTEGER,
                last_updated  TIMESTAMPTZ,
                PRIMARY KEY (icao, lead_bucket)
            )
        """)
    finally:
        con.close()


# ── Math ─────────────────────────────────────────────────────────────────────

def _phi(z: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def fair_no_prob(
    forecast_high: float,
    bias: float,
    sigma: float,
    threshold: float,
    direction: str | None = "higher",
    threshold_hi: float | None = None,
) -> float | None:
    """
    P(NO token wins) under Normal(forecast+bias, sigma).

    direction="higher": NO wins when actual < threshold
    direction="below":  NO wins when actual >= threshold
    direction="range":  NO wins when actual outside [threshold, threshold_hi]
    direction="exact":  treat like a narrow range (±0.5 unit)
    """
    if forecast_high is None or sigma is None or sigma <= 0 or threshold is None:
        return None
    mu = forecast_high + (bias or 0.0)

    if direction in (None, "higher"):
        # NO wins when actual < threshold
        return _phi((threshold - mu) / sigma)
    if direction == "below":
        # NO wins when actual >= threshold
        return 1.0 - _phi((threshold - mu) / sigma)
    if direction == "range":
        hi = threshold_hi if threshold_hi is not None else threshold
        lo, hi = min(threshold, hi), max(threshold, hi)
        # P(actual in [lo, hi]) is YES; NO is the complement.
        p_in = _phi((hi - mu) / sigma) - _phi((lo - mu) / sigma)
        return 1.0 - max(0.0, min(1.0, p_in))
    if direction == "exact":
        width = 0.5
        lo, hi = threshold - width, threshold + width
        p_in = _phi((hi - mu) / sigma) - _phi((lo - mu) / sigma)
        return 1.0 - max(0.0, min(1.0, p_in))
    # Unknown direction: fall back to "higher" semantics.
    return _phi((threshold - mu) / sigma)


def get_bias(con: duckdb.DuckDBPyConnection, icao: str, source: str) -> tuple[float, int]:
    row = con.execute(
        "SELECT bias, n FROM bias WHERE icao = ? AND source = ?",
        [icao, source],
    ).fetchone()
    if row is None:
        return DEFAULT_BIAS, 0
    return float(row[0] or 0.0), int(row[1] or 0)


def score_bucket_readonly(
    *,
    icao: str | None,
    forecast_high: float | None,
    threshold: float | None,
    threshold_hi: float | None,
    direction: str | None,
    hours_to_resolution: float | None,
    source_used: str | None = None,
) -> dict[str, float | int | str | None]:
    """
    Read-only model query — returns calibrated mu/sigma + fair_no_prob for a
    single bucket without inserting into the scans table.

    Used by seller_bot to make fade decisions without polluting the scan
    history. If `source_used` is None, callers should set it to whichever
    forecast they're passing (default 'openmeteo').

    Returns dict with keys: forecast, bias, sigma, mu, fair_no_prob,
    fair_yes_prob, lead_bucket, bias_n, sigma_n, source_used. Values may be
    None when inputs are insufficient (no forecast, no station).
    """
    bucket = lead_bucket(hours_to_resolution)
    out: dict[str, float | int | str | None] = {
        "forecast": forecast_high,
        "bias": None,
        "sigma": None,
        "mu": None,
        "fair_no_prob": None,
        "fair_yes_prob": None,
        "lead_bucket": bucket,
        "bias_n": 0,
        "sigma_n": 0,
        "source_used": source_used or ("openmeteo" if forecast_high is not None else None),
    }
    if forecast_high is None or threshold is None or not icao:
        return out

    src = out["source_used"]
    # If the model DB hasn't been created yet (no weather_bot scans ever ran),
    # fall back to default priors instead of crashing. Same behavior as a
    # cold-start station with no observations.
    try:
        con = _connect(read_only=True)
    except Exception:
        bias, bn = DEFAULT_BIAS, 0
        sigma, sn = DEFAULT_SIGMA_BY_LEAD.get(bucket, 2.5), 0
    else:
        try:
            bias, bn = get_bias(con, icao, src)
            sigma, sn = get_sigma(con, icao, bucket)
        finally:
            con.close()

    mu = forecast_high + bias
    fp = fair_no_prob(
        forecast_high=forecast_high,
        bias=bias,
        sigma=sigma,
        threshold=threshold,
        direction=direction,
        threshold_hi=threshold_hi,
    )
    out.update({
        "bias": bias,
        "sigma": sigma,
        "mu": mu,
        "fair_no_prob": fp,
        "fair_yes_prob": (1.0 - fp) if fp is not None else None,
        "bias_n": bn,
        "sigma_n": sn,
    })
    return out


def get_sigma(con: duckdb.DuckDBPyConnection, icao: str, bucket: str) -> tuple[float, int]:
    row = con.execute(
        "SELECT mse, n FROM sigma WHERE icao = ? AND lead_bucket = ?",
        [icao, bucket],
    ).fetchone()
    if row is None or row[0] is None:
        return DEFAULT_SIGMA_BY_LEAD.get(bucket, 2.5), 0
    mse = float(row[0])
    n = int(row[1] or 0)
    if mse <= 0 or n == 0:
        return DEFAULT_SIGMA_BY_LEAD.get(bucket, 2.5), n
    return math.sqrt(mse), n


# ── Scan recording ───────────────────────────────────────────────────────────

@dataclass
class ScanRecord:
    city: str
    icao: str | None
    event_date: str
    event_slug: str | None
    question: str
    token_id: str | None
    yes_token_id: str | None
    resolution_dt: datetime | None
    unit: str | None
    threshold: float | None
    threshold_hi: float | None
    direction: str | None
    openmeteo_high: float | None
    noaa_high: float | None
    taf_high: float | None
    metar_current: float | None
    metar_max_so_far: float | None
    no_bid: float | None
    no_ask: float | None
    yes_bid: float | None
    yes_ask: float | None


# Forecast-source priority when computing `source_used`. NOAA NWS is the gold
# standard for US stations (dedicated human-edited forecasts); TAF is an
# airport-issued, station-specific forecast available globally but typically
# only for ~30h-period international TAFs; Open-Meteo is the global gridded
# fallback that always works.
_SOURCE_PRIORITY: tuple[tuple[str, str], ...] = (
    ("noaa", "noaa_high"),
    ("taf", "taf_high"),
    ("openmeteo", "openmeteo_high"),
)


def _pick_forecast(rec: "ScanRecord") -> tuple[str | None, float | None]:
    for src, attr in _SOURCE_PRIORITY:
        v = getattr(rec, attr, None)
        if v is not None:
            return src, v
    return None, None


def record_scan(rec: ScanRecord) -> dict[str, Any]:
    """
    Insert one scan row. Computes calibrated mu/sigma and fair NO probability
    against the current bias/sigma for the station. Returns the computed
    model output so the caller can log / display / compare to market.
    """
    now = datetime.now(timezone.utc)
    hours_to_res = None
    if rec.resolution_dt is not None:
        hours_to_res = (rec.resolution_dt - now).total_seconds() / 3600.0
    bucket = lead_bucket(hours_to_res)

    con = _connect()
    try:
        source_used, forecast = _pick_forecast(rec)

        bias = 0.0
        sigma = None
        fair_p = None
        calibrated_mu = None
        edge_bps = None

        if forecast is not None and rec.icao:
            bias, _ = get_bias(con, rec.icao, source_used)
            sigma, _ = get_sigma(con, rec.icao, bucket)
            calibrated_mu = forecast + bias
            fair_p = fair_no_prob(
                forecast_high=forecast,
                bias=bias,
                sigma=sigma,
                threshold=rec.threshold,
                direction=rec.direction,
                threshold_hi=rec.threshold_hi,
            )
            if fair_p is not None and rec.no_ask is not None:
                edge_bps = (rec.no_ask - fair_p) * 10000.0

        con.execute(
            """
            INSERT INTO scans (
                id, scanned_at, city, icao, event_date, event_slug,
                question, token_id, yes_token_id, resolution_dt, hours_to_res,
                lead_bucket, unit, threshold, threshold_hi, direction,
                openmeteo_high, noaa_high, taf_high,
                metar_current, metar_max_so_far,
                no_bid, no_ask, yes_bid, yes_ask,
                calibrated_mu, calibrated_sigma, fair_no_prob, edge_bps,
                bias_used, source_used
            ) VALUES (
                nextval('seq_scan'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                now, rec.city, rec.icao, rec.event_date, rec.event_slug,
                rec.question, rec.token_id, rec.yes_token_id,
                rec.resolution_dt, hours_to_res, bucket,
                rec.unit, rec.threshold, rec.threshold_hi, rec.direction,
                rec.openmeteo_high, rec.noaa_high, rec.taf_high,
                rec.metar_current, rec.metar_max_so_far,
                rec.no_bid, rec.no_ask, rec.yes_bid, rec.yes_ask,
                calibrated_mu, sigma, fair_p, edge_bps,
                bias, source_used,
            ],
        )
        con.commit()
        return {
            "forecast": forecast,
            "source": source_used,
            "bias": bias,
            "sigma": sigma,
            "calibrated_mu": calibrated_mu,
            "fair_no_prob": fair_p,
            "edge_bps": edge_bps,
            "lead_bucket": bucket,
        }
    finally:
        con.close()


def record_scan_batch(recs: Iterable[ScanRecord]) -> list[dict[str, Any]]:
    """
    Insert many scans in one transaction. Returns the same per-record model
    output dicts that `record_scan` returns, in input order.
    """
    out: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    con = _connect()
    try:
        rows_to_insert: list[tuple] = []
        for rec in recs:
            hours_to_res = None
            if rec.resolution_dt is not None:
                hours_to_res = (rec.resolution_dt - now).total_seconds() / 3600.0
            bucket = lead_bucket(hours_to_res)

            source_used, forecast = _pick_forecast(rec)

            bias = 0.0
            sigma = None
            fair_p = None
            calibrated_mu = None
            edge_bps = None

            if forecast is not None and rec.icao:
                bias, _ = get_bias(con, rec.icao, source_used)
                sigma, _ = get_sigma(con, rec.icao, bucket)
                calibrated_mu = forecast + bias
                fair_p = fair_no_prob(
                    forecast_high=forecast,
                    bias=bias,
                    sigma=sigma,
                    threshold=rec.threshold,
                    direction=rec.direction,
                    threshold_hi=rec.threshold_hi,
                )
                if fair_p is not None and rec.no_ask is not None:
                    edge_bps = (rec.no_ask - fair_p) * 10000.0

            rows_to_insert.append((
                now, rec.city, rec.icao, rec.event_date, rec.event_slug,
                rec.question, rec.token_id, rec.yes_token_id,
                rec.resolution_dt, hours_to_res, bucket,
                rec.unit, rec.threshold, rec.threshold_hi, rec.direction,
                rec.openmeteo_high, rec.noaa_high, rec.taf_high,
                rec.metar_current, rec.metar_max_so_far,
                rec.no_bid, rec.no_ask, rec.yes_bid, rec.yes_ask,
                calibrated_mu, sigma, fair_p, edge_bps,
                bias, source_used,
            ))
            out.append({
                "city": rec.city,
                "forecast": forecast,
                "source": source_used,
                "bias": bias,
                "sigma": sigma,
                "calibrated_mu": calibrated_mu,
                "fair_no_prob": fair_p,
                "edge_bps": edge_bps,
                "lead_bucket": bucket,
            })
        if rows_to_insert:
            con.executemany(
                """
                INSERT INTO scans (
                    id, scanned_at, city, icao, event_date, event_slug,
                    question, token_id, yes_token_id, resolution_dt, hours_to_res,
                    lead_bucket, unit, threshold, threshold_hi, direction,
                    openmeteo_high, noaa_high, taf_high,
                    metar_current, metar_max_so_far,
                    no_bid, no_ask, yes_bid, yes_ask,
                    calibrated_mu, calibrated_sigma, fair_no_prob, edge_bps,
                    bias_used, source_used
                ) VALUES (
                    nextval('seq_scan'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                rows_to_insert,
            )
            con.commit()
    finally:
        con.close()
    return out


# ── Outcome + self-tuning back-propagation ───────────────────────────────────

def _update_bias(con: duckdb.DuckDBPyConnection, icao: str, source: str,
                 residual: float, now: datetime) -> None:
    row = con.execute(
        "SELECT bias, n FROM bias WHERE icao = ? AND source = ?",
        [icao, source],
    ).fetchone()
    if row is None:
        con.execute(
            "INSERT INTO bias VALUES (?, ?, ?, ?, ?)",
            [icao, source, residual, 1, now],
        )
        return
    prev_bias, n = float(row[0] or 0.0), int(row[1] or 0)
    n_new = n + 1
    # Warm-up: uniform average for early samples; then exponential.
    alpha = 1.0 / n_new if n_new <= WARMUP_SAMPLES else ALPHA_BIAS
    new_bias = alpha * residual + (1.0 - alpha) * prev_bias
    con.execute(
        "UPDATE bias SET bias = ?, n = ?, last_updated = ? WHERE icao = ? AND source = ?",
        [new_bias, n_new, now, icao, source],
    )


def _update_sigma(con: duckdb.DuckDBPyConnection, icao: str, bucket: str,
                  residual: float, now: datetime) -> None:
    row = con.execute(
        "SELECT mse, n FROM sigma WHERE icao = ? AND lead_bucket = ?",
        [icao, bucket],
    ).fetchone()
    r2 = residual * residual
    if row is None:
        con.execute(
            "INSERT INTO sigma VALUES (?, ?, ?, ?, ?)",
            [icao, bucket, r2, 1, now],
        )
        return
    prev_mse, n = float(row[0] or 0.0), int(row[1] or 0)
    n_new = n + 1
    alpha = 1.0 / n_new if n_new <= WARMUP_SAMPLES else ALPHA_MSE
    new_mse = alpha * r2 + (1.0 - alpha) * prev_mse
    con.execute(
        "UPDATE sigma SET mse = ?, n = ?, last_updated = ? WHERE icao = ? AND lead_bucket = ?",
        [new_mse, n_new, now, icao, bucket],
    )


def update_from_outcome(icao: str, event_date: str, actual_high: float) -> dict[str, Any]:
    """
    Back-propagate: for each source (openmeteo, noaa), look up the most recent
    scan that captured that source's forecast for this (icao, event_date),
    compute residual = actual - forecast, update bias and sigma EMAs.

    Picks the *earliest* scan per source as training data, since that's the
    longest-lead forecast and exercises sigma at the hardest bucket. Using
    every scan would over-weight stations we see many times per day.
    """
    now = datetime.now(timezone.utc)
    con = _connect()
    updated: dict[str, Any] = {"icao": icao, "event_date": event_date,
                                "actual": actual_high, "updates": []}
    try:
        # Pick one scan per source — the one with the longest hours_to_res that
        # still has the forecast value for that source. That way sigma is
        # trained against lead-time the market actually cared about.
        for source, col in (
            ("openmeteo", "openmeteo_high"),
            ("noaa", "noaa_high"),
            ("taf", "taf_high"),
        ):
            row = con.execute(
                f"""
                SELECT {col}, hours_to_res, lead_bucket
                FROM scans
                WHERE icao = ? AND event_date = ? AND {col} IS NOT NULL
                ORDER BY hours_to_res DESC NULLS LAST
                LIMIT 1
                """,
                [icao, event_date],
            ).fetchone()
            if row is None:
                continue
            forecast = float(row[0])
            bucket = row[2] or lead_bucket(row[1])
            residual = actual_high - forecast
            _update_bias(con, icao, source, residual, now)
            _update_sigma(con, icao, bucket, residual, now)
            updated["updates"].append({
                "source": source,
                "forecast": forecast,
                "residual": round(residual, 3),
                "bucket": bucket,
            })
        con.commit()
    finally:
        con.close()
    return updated


def record_outcome(
    *,
    icao: str,
    event_date: str,
    city: str,
    unit: str | None,
    actual_high: float,
    bucket_lo: float | None = None,
    bucket_hi: float | None = None,
    source: str = "polymarket_bucket",
    event_slug: str | None = None,
) -> dict[str, Any]:
    """Insert outcome (idempotent) and trigger back-propagation."""
    now = datetime.now(timezone.utc)
    con = _connect()
    try:
        existed = con.execute(
            "SELECT 1 FROM outcomes WHERE icao = ? AND event_date = ?",
            [icao, event_date],
        ).fetchone() is not None
        if existed:
            return {"icao": icao, "event_date": event_date, "status": "already-recorded"}
        con.execute(
            """
            INSERT INTO outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [icao, event_date, city, unit, actual_high,
             bucket_lo, bucket_hi, source, event_slug, now],
        )
        con.commit()
    finally:
        con.close()
    updates = update_from_outcome(icao, event_date, actual_high)
    updates["status"] = "recorded"
    return updates


# ── Summaries for the UI ─────────────────────────────────────────────────────

def list_cities() -> list[dict[str, Any]]:
    con = _connect(read_only=True)
    try:
        rows = con.execute("""
            SELECT city,
                   ANY_VALUE(icao) AS icao,
                   COUNT(*) AS n_scans,
                   MAX(scanned_at) AS last_seen
            FROM scans
            GROUP BY city
            ORDER BY last_seen DESC
        """).fetchall()
    finally:
        con.close()
    return [{"city": r[0], "icao": r[1], "n_scans": int(r[2] or 0),
             "last_seen": r[3].isoformat() if r[3] else None}
            for r in rows]


def list_scans(city: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
    con = _connect(read_only=True)
    try:
        if city:
            rows = con.execute("""
                SELECT scanned_at, city, icao, event_date, question, unit,
                       threshold, threshold_hi, direction,
                       openmeteo_high, noaa_high, taf_high,
                       metar_current, metar_max_so_far,
                       no_bid, no_ask, yes_bid, yes_ask,
                       calibrated_mu, calibrated_sigma, fair_no_prob, edge_bps,
                       bias_used, source_used, hours_to_res, lead_bucket
                FROM scans
                WHERE city = ?
                ORDER BY scanned_at DESC
                LIMIT ?
            """, [city, limit]).fetchall()
        else:
            rows = con.execute("""
                SELECT scanned_at, city, icao, event_date, question, unit,
                       threshold, threshold_hi, direction,
                       openmeteo_high, noaa_high, taf_high,
                       metar_current, metar_max_so_far,
                       no_bid, no_ask, yes_bid, yes_ask,
                       calibrated_mu, calibrated_sigma, fair_no_prob, edge_bps,
                       bias_used, source_used, hours_to_res, lead_bucket
                FROM scans
                ORDER BY scanned_at DESC
                LIMIT ?
            """, [limit]).fetchall()
    finally:
        con.close()
    keys = ["scanned_at", "city", "icao", "event_date", "question", "unit",
            "threshold", "threshold_hi", "direction",
            "openmeteo_high", "noaa_high", "taf_high",
            "metar_current", "metar_max_so_far",
            "no_bid", "no_ask", "yes_bid", "yes_ask",
            "calibrated_mu", "calibrated_sigma", "fair_no_prob", "edge_bps",
            "bias_used", "source_used", "hours_to_res", "lead_bucket"]
    out = []
    for r in rows:
        d = dict(zip(keys, r))
        if d["scanned_at"] is not None:
            d["scanned_at"] = d["scanned_at"].isoformat()
        out.append(d)
    return out


def list_outcomes(city: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
    con = _connect(read_only=True)
    try:
        if city:
            rows = con.execute("""
                SELECT icao, event_date, city, unit, actual_high,
                       bucket_lo, bucket_hi, source, event_slug, resolved_at
                FROM outcomes
                WHERE city = ?
                ORDER BY event_date DESC
                LIMIT ?
            """, [city, limit]).fetchall()
        else:
            rows = con.execute("""
                SELECT icao, event_date, city, unit, actual_high,
                       bucket_lo, bucket_hi, source, event_slug, resolved_at
                FROM outcomes
                ORDER BY resolved_at DESC
                LIMIT ?
            """, [limit]).fetchall()
    finally:
        con.close()
    keys = ["icao", "event_date", "city", "unit", "actual_high",
            "bucket_lo", "bucket_hi", "source", "event_slug", "resolved_at"]
    out = []
    for r in rows:
        d = dict(zip(keys, r))
        if d["resolved_at"] is not None:
            d["resolved_at"] = d["resolved_at"].isoformat()
        out.append(d)
    return out


def list_stations() -> list[dict[str, Any]]:
    """Per-(icao, source) bias table and per-(icao, lead_bucket) sigma table."""
    con = _connect(read_only=True)
    try:
        bias_rows = con.execute(
            "SELECT icao, source, bias, n, last_updated FROM bias ORDER BY icao, source"
        ).fetchall()
        sigma_rows = con.execute(
            "SELECT icao, lead_bucket, mse, n, last_updated FROM sigma ORDER BY icao, lead_bucket"
        ).fetchall()
    finally:
        con.close()
    bias = [{
        "icao": r[0], "source": r[1], "bias": float(r[2]) if r[2] is not None else None,
        "n": int(r[3] or 0), "last_updated": r[4].isoformat() if r[4] else None,
    } for r in bias_rows]
    sigma = [{
        "icao": r[0], "lead_bucket": r[1],
        "sigma": math.sqrt(float(r[2])) if r[2] is not None and r[2] > 0 else None,
        "n": int(r[3] or 0), "last_updated": r[4].isoformat() if r[4] else None,
    } for r in sigma_rows]
    return {"bias": bias, "sigma": sigma}


def calibration_summary() -> dict[str, Any]:
    """
    Join scans → outcomes, then for each scan compute whether the NO outcome
    actually came true, bucket the scan's fair_no_prob into deciles, and
    report (bucket, n, predicted_mean, empirical_hit_rate).
    """
    con = _connect(read_only=True)
    try:
        rows = con.execute("""
            SELECT s.fair_no_prob, s.threshold, s.threshold_hi, s.direction,
                   o.actual_high
            FROM scans s
            JOIN outcomes o ON o.icao = s.icao AND o.event_date = s.event_date
            WHERE s.fair_no_prob IS NOT NULL
              AND o.actual_high IS NOT NULL
              AND s.threshold IS NOT NULL
        """).fetchall()
    finally:
        con.close()

    def _no_hit(actual: float, thr: float, thr_hi: float | None, direction: str | None) -> bool:
        if direction in (None, "higher"):
            return actual < thr
        if direction == "below":
            return actual >= thr
        if direction == "range":
            hi = thr_hi if thr_hi is not None else thr
            lo, hi = min(thr, hi), max(thr, hi)
            return not (lo <= actual <= hi)
        if direction == "exact":
            return not (thr - 0.5 <= actual <= thr + 0.5)
        return actual < thr

    buckets: dict[int, list[tuple[float, bool]]] = {i: [] for i in range(10)}
    for p, thr, thr_hi, direction, actual in rows:
        bucket_idx = min(9, max(0, int(float(p) * 10)))
        buckets[bucket_idx].append((float(p), _no_hit(float(actual), float(thr),
                                                     float(thr_hi) if thr_hi is not None else None,
                                                     direction)))
    out = []
    for idx in sorted(buckets.keys()):
        items = buckets[idx]
        if not items:
            continue
        avg_pred = sum(p for p, _ in items) / len(items)
        empirical = sum(1 for _, hit in items if hit) / len(items)
        out.append({
            "bucket": idx,
            "bucket_lo": idx / 10.0,
            "bucket_hi": (idx + 1) / 10.0,
            "n": len(items),
            "predicted_mean": round(avg_pred, 4),
            "empirical_hit_rate": round(empirical, 4),
        })
    return {"buckets": out, "total_paired": sum(len(v) for v in buckets.values())}


# ── Polymarket outcome backfill ──────────────────────────────────────────────

def _parse_bucket_range(question: str) -> tuple[float | None, float | None, str | None]:
    """
    Extract (lo, hi, unit) from a bucket-market question like
        "68-70°F"
        "22.5°C or higher"
        "below 12°C"
    Returns (None, None, None) if we can't.
    """
    import re as _re
    s = question.strip()
    # "LO-HI°U" — e.g. "68-70°F"
    m = _re.search(r"(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\b", s, _re.IGNORECASE)
    if m:
        return float(m.group(1)), float(m.group(2)), m.group(3).upper()
    # "X°U or higher"
    m = _re.search(r"(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s+or\s+higher", s, _re.IGNORECASE)
    if m:
        # Open-ended upper: use threshold + 5 as the midpoint anchor. Not
        # ideal but only used when the day's high actually ends up in the
        # upper tail; we prefer METAR for that case.
        t = float(m.group(1))
        return t, t + 5.0, m.group(2).upper()
    # "below X°U" / "X°U or lower"
    m = _re.search(r"below\s+(-?\d+(?:\.\d+)?)\s*°?\s*([CF])", s, _re.IGNORECASE)
    if m:
        t = float(m.group(1))
        return t - 5.0, t, m.group(2).upper()
    m = _re.search(r"(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s+or\s+lower", s, _re.IGNORECASE)
    if m:
        t = float(m.group(1))
        return t - 5.0, t, m.group(2).upper()
    # "X°U" bare — single-degree bucket, interpret as [X-0.5, X+0.5).
    # This is the Polymarket default shape (e.g. "14°C") and covers ~65% of
    # closed weather events, so keep it last so the broader patterns above
    # get a chance first.
    m = _re.fullmatch(r"\s*(-?\d+(?:\.\d+)?)\s*°?\s*([CF])\s*", s, _re.IGNORECASE)
    if m:
        t = float(m.group(1))
        return t - 0.5, t + 0.5, m.group(2).upper()
    return None, None, None


def backfill_outcomes_from_polymarket() -> dict[str, Any]:
    """
    For every (icao, event_date) in `scans` whose resolution has passed and
    that isn't in `outcomes` yet, query the Polymarket closed event, find the
    resolved-YES bucket, and store its midpoint as actual_high.

    For non-bucket / single-threshold events where the winner doesn't pin down
    the actual temperature to a range, fall back to METAR daily max.
    """
    from automata.polymarket import _gamma_get, POLYMARKET_EVENTS_URL
    now = datetime.now(timezone.utc)
    stats = {"checked": 0, "resolved": 0, "skipped": 0, "errors": 0, "details": []}

    con = _connect()
    try:
        pending = con.execute("""
            SELECT DISTINCT s.icao, s.event_date, s.city, s.unit, s.event_slug
            FROM scans s
            LEFT JOIN outcomes o
                ON o.icao = s.icao AND o.event_date = s.event_date
            WHERE o.icao IS NULL
              AND s.icao IS NOT NULL
              AND s.resolution_dt IS NOT NULL
              AND s.resolution_dt < ?
        """, [now]).fetchall()
    finally:
        con.close()

    for icao, event_date, city, unit, event_slug in pending:
        stats["checked"] += 1
        if not event_slug:
            stats["skipped"] += 1
            continue
        try:
            resp = _gamma_get(POLYMARKET_EVENTS_URL, {"slug": event_slug, "closed": "true"})
            events = resp.json() or []
        except Exception as exc:
            log.warning("backfill: gamma fetch for %s failed: %s", event_slug, exc)
            stats["errors"] += 1
            continue
        if not isinstance(events, list) or not events:
            stats["skipped"] += 1
            continue
        event = events[0]
        markets = event.get("markets") or []
        winner = None
        for m in markets:
            prices = m.get("outcomePrices")
            if isinstance(prices, str):
                try:
                    import json as _json
                    prices = _json.loads(prices)
                except Exception:
                    prices = None
            if not isinstance(prices, list) or len(prices) < 1:
                continue
            try:
                p_yes = float(prices[0])
            except Exception:
                continue
            if p_yes >= 0.99:
                winner = m
                break
        if winner is None:
            stats["skipped"] += 1
            continue

        q = str(winner.get("groupItemTitle") or winner.get("question") or "")
        lo, hi, u = _parse_bucket_range(q)
        if lo is None or hi is None:
            stats["skipped"] += 1
            continue
        mid = (lo + hi) / 2.0

        try:
            record_outcome(
                icao=icao, event_date=event_date, city=city,
                unit=unit or u, actual_high=mid,
                bucket_lo=lo, bucket_hi=hi,
                source="polymarket_bucket", event_slug=event_slug,
            )
            stats["resolved"] += 1
            stats["details"].append({
                "icao": icao, "event_date": event_date, "city": city,
                "actual": mid, "bucket": f"{lo}-{hi}{u or ''}",
            })
            log.info(
                "[weather-model] outcome %s %s  bucket %s-%s%s mid=%.1f (icao=%s)",
                city, event_date, lo, hi, u or "", mid, icao,
            )
        except Exception as exc:
            log.warning("backfill: record_outcome %s %s failed: %s", icao, event_date, exc)
            stats["errors"] += 1
    return stats
