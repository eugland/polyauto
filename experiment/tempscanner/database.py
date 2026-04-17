"""DuckDB read-only queries for the web UI."""
from __future__ import annotations

import os


def query_cities(db_path: str) -> dict:
    if not os.path.exists(db_path):
        return {"error": "DB not found — start the scanner first.", "sessions": []}
    try:
        import duckdb
        con = duckdb.connect(db_path, read_only=True)
        rows = con.execute("""
            SELECT city, event_date, COUNT(*) AS buckets,
                   MAX(discovered_at) AS last_seen
            FROM markets
            GROUP BY city, event_date
            ORDER BY event_date DESC, city
        """).fetchall()
        con.close()
        return {
            "sessions": [
                {"city": r[0], "event_date": r[1], "buckets": r[2], "last_seen": str(r[3])}
                for r in rows
            ]
        }
    except Exception as exc:
        return {"error": str(exc), "sessions": []}


def query_session(db_path: str, city: str, event_date: str) -> dict:
    if not os.path.exists(db_path):
        return {"error": "DB not found — start the scanner first."}
    try:
        import duckdb
        con = duckdb.connect(db_path, read_only=True)

        buckets_raw = con.execute("""
            SELECT condition_id, question, bucket_lo, bucket_hi, unit, direction,
                   yes_token_id, icao, lat, lon, tz
            FROM markets
            WHERE city = ? AND event_date = ?
            ORDER BY COALESCE(bucket_lo, -9999)
        """, [city, event_date]).fetchall()
        buckets = [
            {"condition_id": r[0], "question": r[1], "bucket_lo": r[2], "bucket_hi": r[3],
             "unit": r[4], "direction": r[5], "yes_token_id": r[6],
             "icao": r[7], "lat": r[8], "lon": r[9], "tz": r[10]}
            for r in buckets_raw
        ]

        snaps_raw = con.execute("""
            SELECT bs.condition_id, bs.sampled_at, bs.local_hour,
                   bs.best_bid, bs.best_ask, bs.mid, bs.spread
            FROM book_snapshots bs
            JOIN markets m ON m.condition_id = bs.condition_id
            WHERE m.city = ? AND m.event_date = ?
            ORDER BY bs.sampled_at
        """, [city, event_date]).fetchall()
        snapshots = [
            {"condition_id": r[0], "sampled_at": str(r[1]), "local_hour": r[2],
             "best_bid": r[3], "best_ask": r[4], "mid": r[5], "spread": r[6]}
            for r in snaps_raw
        ]

        obs_raw = con.execute("""
            SELECT observed_at, temp_c, source FROM weather_obs
            WHERE city = ? AND DATE(observed_at) = ?::DATE
            ORDER BY observed_at
        """, [city, event_date]).fetchall()
        obs = [{"observed_at": str(r[0]), "temp_c": r[1], "source": r[2]} for r in obs_raw]

        fc_raw = con.execute("""
            SELECT sampled_at, forecast_high_c, source FROM forecasts
            WHERE city = ? AND forecast_date = ?
            ORDER BY sampled_at
        """, [city, event_date]).fetchall()
        forecasts = [{"sampled_at": str(r[0]), "forecast_high_c": r[1], "source": r[2]} for r in fc_raw]

        # Latest snapshot per bucket
        latest_raw = con.execute("""
            SELECT bs.condition_id, bs.best_bid, bs.best_ask, bs.mid, bs.sampled_at, bs.local_hour
            FROM book_snapshots bs
            INNER JOIN (
                SELECT condition_id, MAX(sampled_at) AS max_ts
                FROM book_snapshots GROUP BY condition_id
            ) lx ON lx.condition_id = bs.condition_id AND lx.max_ts = bs.sampled_at
        """).fetchall()
        latest = {r[0]: {"best_bid": r[1], "best_ask": r[2], "mid": r[3],
                         "sampled_at": str(r[4]), "local_hour": r[5]}
                  for r in latest_raw}
        for b in buckets:
            b.update(latest.get(b["condition_id"]) or {})

        best_cid = max(
            ((b["condition_id"], b.get("mid") or 0) for b in buckets),
            key=lambda x: x[1], default=(None, 0)
        )[0]

        con.close()
        return {
            "city": city, "event_date": event_date,
            "buckets": buckets, "snapshots": snapshots,
            "weather_obs": obs, "forecasts": forecasts,
            "best_bucket_cid": best_cid,
            "latest_obs_c": obs[-1]["temp_c"] if obs else None,
            "latest_forecast_c": forecasts[-1]["forecast_high_c"] if forecasts else None,
        }
    except Exception as exc:
        return {"error": str(exc)}
