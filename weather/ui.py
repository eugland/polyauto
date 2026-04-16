from __future__ import annotations

import argparse
import os
from collections import deque
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, request

from automata.client import get_best_books_bulk
from automata.weather_bot import CITY_TZ
from weather.bet import pick_best_executable, should_skip_city, suggested_bid
from weather.forecast import attach_probabilities
from weather.markets import RankedMarket, fetch_ranked_markets

SIGNAL_LOG_PATH = Path("experiment") / "logs" / "weather_signals.log"
ETH_LOG_PATH    = Path("experiment") / "logs" / "eth_1h.log"


def _attach_prices(ranked: list[RankedMarket], host: str) -> None:
    token_ids: list[str] = []
    for rm in ranked:
        for br in rm.brackets:
            token_ids.append(br.no_token_id)
            if br.yes_token_id:
                token_ids.append(br.yes_token_id)
    if not token_ids:
        return
    books = get_best_books_bulk(host, token_ids)
    for rm in ranked:
        for br in rm.brackets:
            book = books.get(br.no_token_id, {}) or {}
            br.bid = book.get("bid")
            br.ask = book.get("ask")
            if br.yes_token_id:
                ybook = books.get(br.yes_token_id, {}) or {}
                br.yes_bid = ybook.get("bid")
                br.yes_ask = ybook.get("ask")


def _fmt_money(v: float | None) -> str:
    if v is None:
        return "n/a"
    if v >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v/1_000:.2f}k"
    return f"{v:.2f}"


def _decide_text(rm: RankedMarket) -> str:
    if rm.decide_utc is None:
        return "decide=?"
    tz_name = CITY_TZ.get(rm.city, "UTC")
    local = rm.decide_utc.astimezone(ZoneInfo(tz_name))
    mins = rm.minutes_until_decide or 0
    rel = f"{mins/60:+.1f}h" if abs(mins) >= 60 else f"{mins:+.0f}m"
    return f"{local.strftime('%Y-%m-%d %H:%M %Z')} ({rel})"


def _fetch_snapshot(top: int, city: str | None) -> dict:
    host = os.getenv("POLYMARKET_HOST", "https://clob.polymarket.com")
    min_no_price = float(os.getenv("MIN_NO_PRICE", "0.97"))
    max_no_price = float(os.getenv("MAX_NO_PRICE", "0.998"))

    ranked = fetch_ranked_markets()
    if city:
        ranked = [rm for rm in ranked if rm.city.lower() == city.lower()]
    if top > 0:
        ranked = ranked[:top]

    _attach_prices(ranked, host)
    attach_probabilities(ranked)

    picks: dict[str, tuple[RankedMarket, str]] = {}
    for rm in ranked:
        if should_skip_city(rm.city):
            continue
        best = pick_best_executable(rm, min_no_price=min_no_price, max_no_price=max_no_price)
        if best is None:
            continue
        br, ev = best
        key = rm.city + "|" + rm.event_date
        prev = picks.get(key)
        if prev is None:
            picks[key] = (rm, br.no_token_id)
            continue
        # Keep best EV if duplicate city/date shows up.
        prev_rm, prev_no_id = prev
        prev_br = next((b for b in prev_rm.brackets if b.no_token_id == prev_no_id), None)
        prev_ev = -1e9
        if prev_br is not None and prev_br.p_yes is not None:
            px = suggested_bid(prev_br)
            if px is not None:
                prev_ev = (1.0 - prev_br.p_yes) - px
        if ev > prev_ev:
            picks[key] = (rm, br.no_token_id)

    markets: list[dict] = []
    for rm in ranked:
        key = rm.city + "|" + rm.event_date
        picked_no_id = picks[key][1] if key in picks and picks[key][0] is rm else None
        with_yes_bid = [b for b in rm.brackets if b.yes_bid is not None]
        pred = max(with_yes_bid, key=lambda b: b.yes_bid) if with_yes_bid else None
        fmean = rm.brackets[0].forecast_mean if rm.brackets else None
        fstd = rm.brackets[0].forecast_std if rm.brackets else None

        rows: list[dict] = []
        for br in sorted([b for b in rm.brackets if b.ask is not None], key=lambda b: b.threshold):
            py = br.p_yes
            rows.append(
                {
                    "question": br.question,
                    "no_ask": br.ask,
                    "no_bid": br.bid,
                    "yes_ask": br.yes_ask,
                    "yes_bid": br.yes_bid,
                    "p_yes": py,
                    "p_no": (1.0 - py) if py is not None else None,
                    "delta": (br.threshold - pred.threshold) if pred is not None else None,
                    "vol": _fmt_money(br.volume),
                    "liq": _fmt_money(br.liquidity),
                    "my_bid": suggested_bid(br),
                    "pick": picked_no_id == br.no_token_id,
                }
            )

        markets.append(
            {
                "city": rm.city,
                "title_date": rm.title_date,
                "icao": rm.icao or "?",
                "forecast_mean": fmean,
                "forecast_std": fstd,
                "unit": rm.unit,
                "decide": _decide_text(rm),
                "model_threshold": pred.threshold if pred else None,
                "model_yes_bid": pred.yes_bid if pred else None,
                "vol": _fmt_money(sum(b.volume or 0.0 for b in rm.brackets)),
                "liq": _fmt_money(sum(b.liquidity or 0.0 for b in rm.brackets)),
                "rows": rows,
            }
        )
    return {"markets": markets, "generated_at": datetime.now().isoformat(timespec="seconds")}


def _tail_file(path: Path, lines: int) -> list[str]:
    if not path.exists():
        return [f"Log not found: {path}"]
    with path.open("r", encoding="utf-8", errors="replace") as f:
        return list(deque((ln.rstrip("\n") for ln in f), maxlen=max(1, lines)))


def _tail_signal_logs(lines: int = 200) -> list[str]:
    return _tail_file(SIGNAL_LOG_PATH, lines)


def _tail_eth_logs(lines: int = 200) -> list[str]:
    return _tail_file(ETH_LOG_PATH, lines)


HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Weather Bets UI</title>
  <style>
    :root { --bg:#f7f4ed; --ink:#1e1b18; --muted:#6e655d; --card:#fffdf8; --line:#d7cdbf; --accent:#0f766e; --pick:#14532d; }
    body { margin:0; font-family: "IBM Plex Sans", "Segoe UI", sans-serif; background:linear-gradient(120deg,#f8f5ef,#efe9dd); color:var(--ink); }
    .wrap { max-width:1300px; margin:20px auto; padding:0 14px; }
    h1 { margin:0 0 6px 0; font-family:"Space Grotesk","Segoe UI",sans-serif; font-size:28px; }
    .muted { color:var(--muted); font-size:13px; }
    .grid { display:grid; grid-template-columns:2fr 1fr; gap:14px; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; box-shadow:0 6px 18px rgba(30,27,24,.06); }
    .mkt { margin-bottom:16px; padding-bottom:12px; border-bottom:1px dashed var(--line); }
    .mkt:last-child { margin-bottom:0; border-bottom:none; }
    .title { font-weight:700; font-size:16px; margin-bottom:4px; }
    .sub { font-size:12px; color:var(--muted); margin-bottom:8px; }
    table { width:100%; border-collapse:collapse; font-size:12px; }
    th, td { padding:5px 4px; border-bottom:1px solid #eee5d9; text-align:right; white-space:nowrap; }
    th:first-child, td:first-child { text-align:left; max-width:220px; overflow:hidden; text-overflow:ellipsis; }
    tr.pick { background:#e7f7e9; }
    .logs { height:38vh; overflow:auto; background:#18181b; color:#d4d4d8; border-radius:8px; padding:10px; font-family:Consolas,monospace; font-size:12px; }
    .log-line { margin-bottom:3px; white-space:pre-wrap; word-break:break-all; }
    .cand { color:#93c5fd; }
    .bet { color:#86efac; font-weight:700; }
    .skip { color:#fca5a5; }
    .enter { color:#86efac; font-weight:700; }
    .skip-risk { color:#fca5a5; }
    .no-sig { color:#a1a1aa; }
    .warn { color:#fbbf24; }
    .err { color:#f87171; font-weight:700; }
    .stack > .card + .card { margin-top:12px; }
    @media (max-width: 980px) { .grid { grid-template-columns:1fr; } .logs { height:240px; } }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Weather Candidate Board</h1>
    <div class="muted">Shows current candidates + picks and signal log (candidate / placed / skip only). Auto refresh: 20s.</div>
    <div class="grid" style="margin-top:12px;">
      <div class="card">
        <div id="stamp" class="muted"></div>
        <div id="markets"></div>
      </div>
      <div class="stack">
        <div class="card">
          <div style="font-weight:700; margin-bottom:8px;">Weather Signal Log
            <span class="muted" style="font-weight:normal; font-size:11px;">(experiment/logs/weather_signals.log)</span>
          </div>
          <div id="logs" class="logs"></div>
        </div>
        <div class="card">
          <div style="font-weight:700; margin-bottom:8px;">ETH 1H Daemon Log
            <span class="muted" style="font-weight:normal; font-size:11px;">(experiment/logs/eth_1h.log)</span>
          </div>
          <div id="eth_logs" class="logs"></div>
        </div>
      </div>
    </div>
  </div>
<script>
function c(v){ return v===null||v===undefined ? "n/a" : v; }
function cent(v){ return v===null||v===undefined ? "n/a" : (v*100).toFixed(2)+"¢"; }
function pct(v){ return v===null||v===undefined ? "n/a" : (v*100).toFixed(2)+"%"; }
function delta(v){ return v===null||v===undefined ? "n/a" : (v>0?"+":"")+v.toFixed(0)+"°"; }

async function loadMarkets(){
  const d = await fetch("/api/markets").then(r=>r.json());
  document.getElementById("stamp").textContent = "Generated: " + d.generated_at;
  const host = document.getElementById("markets");
  host.innerHTML = d.markets.map(m => `
    <div class="mkt">
      <div class="title">━━━ ${m.city}  ${m.title_date}  [${m.icao}]</div>
      <div class="sub">forecast=${c(m.forecast_mean)?.toFixed ? m.forecast_mean.toFixed(1):"n/a"}°${m.unit} ±${c(m.forecast_std)?.toFixed ? m.forecast_std.toFixed(1):"n/a"} | decide=${m.decide} | model=${m.model_threshold ?? "n/a"}° (${cent(m.model_yes_bid)}) | vol=$${m.vol} liq=$${m.liq}</div>
      <table>
        <thead><tr><th>bracket</th><th>NO ask</th><th>NO bid</th><th>YES ask</th><th>YES bid</th><th>P(yes)</th><th>P(no)</th><th>Δ°</th><th>$vol</th><th>$liq</th><th>my bid</th><th>pick</th></tr></thead>
        <tbody>
          ${m.rows.map(r => `<tr class="${r.pick?'pick':''}"><td>${r.question}</td><td>${cent(r.no_ask)}</td><td>${cent(r.no_bid)}</td><td>${cent(r.yes_ask)}</td><td>${cent(r.yes_bid)}</td><td>${pct(r.p_yes)}</td><td>${pct(r.p_no)}</td><td>${delta(r.delta)}</td><td>$${r.vol}</td><td>$${r.liq}</td><td>${cent(r.my_bid)}</td><td>${r.pick?"*":""}</td></tr>`).join("")}
        </tbody>
      </table>
    </div>`).join("");
}

function escapeHtml(s){
  return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

async function loadLogs(){
  const d = await fetch("/api/logs").then(r=>r.json());
  const box = document.getElementById("logs");
  box.innerHTML = d.lines.map(l => {
    let cls = "log-line";
    if (l.includes("BET_PLACED")) cls += " bet";
    else if (l.includes("CANDIDATE")) cls += " cand";
    else if (l.includes("SKIP")) cls += " skip";
    return `<div class="${cls}">${escapeHtml(l)}</div>`;
  }).join("");
  box.scrollTop = box.scrollHeight;
}

async function loadEthLogs(){
  const d = await fetch("/api/eth_logs").then(r=>r.json());
  const box = document.getElementById("eth_logs");
  box.innerHTML = d.lines.map(l => {
    let cls = "log-line";
    if (l.includes("ENTER-"))            cls += " enter";
    else if (l.includes("Bought "))      cls += " bet";
    else if (l.includes("SKIP-RISK"))    cls += " skip-risk";
    else if (l.includes("NO-SIGNAL"))    cls += " no-sig";
    else if (l.includes("STOP-LOSS"))    cls += " err";
    else if (/\bERROR\b/.test(l))        cls += " err";
    else if (/\bWARNING\b/.test(l))      cls += " warn";
    return `<div class="${cls}">${escapeHtml(l)}</div>`;
  }).join("");
  box.scrollTop = box.scrollHeight;
}

async function tick(){
  await Promise.all([loadMarkets(), loadLogs(), loadEthLogs()]);
}
tick();
setInterval(tick, 20000);
</script>
</body>
</html>
"""


def create_app() -> Flask:
    app = Flask(__name__)

    @app.route("/")
    def index():
        return render_template_string(HTML)

    @app.route("/api/markets")
    def api_markets():
        top = int(request.args.get("top", "20"))
        city = request.args.get("city")
        return jsonify(_fetch_snapshot(top=top, city=city))

    @app.route("/api/logs")
    def api_logs():
        lines = int(request.args.get("lines", "200"))
        return jsonify({"lines": _tail_signal_logs(lines)})

    @app.route("/api/eth_logs")
    def api_eth_logs():
        lines = int(request.args.get("lines", "200"))
        return jsonify({"lines": _tail_eth_logs(lines)})

    return app


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Weather UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5060)
    args = parser.parse_args()
    app = create_app()
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()

