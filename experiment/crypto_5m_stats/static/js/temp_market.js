// ── Highest-Temp Market UI ──────────────────────────────────────────────────

let tempLoaded = false;
let tempChart = null;
let tempPnlChart = null;

// Palette for up to ~20 buckets
const BUCKET_PALETTE = [
  "#60a5fa","#34d399","#fbbf24","#f87171","#a78bfa",
  "#fb923c","#2dd4bf","#e879f9","#facc15","#4ade80",
  "#38bdf8","#f472b6","#c084fc","#86efac","#fda4af",
  "#67e8f9","#fcd34d","#d8b4fe","#6ee7b7","#fca5a5",
];

async function loadTempMarket() {
  if (tempLoaded) return;
  tempLoaded = true;
  const resp = await fetch("/api/temp-market/cities").then(r => r.json());
  const sel = document.getElementById("temp-session-select");
  if (resp.error) {
    document.getElementById("temp-meta").textContent = resp.error;
    return;
  }
  resp.sessions.forEach(s => {
    const opt = document.createElement("option");
    opt.value = `${s.city}||${s.event_date}`;
    opt.textContent = `${s.city}  —  ${s.event_date}  (${s.buckets} buckets)`;
    sel.appendChild(opt);
  });
  document.getElementById("temp-meta").textContent =
    `${resp.sessions.length} session(s) available`;

  // Auto-select latest
  if (resp.sessions.length > 0) {
    sel.value = `${resp.sessions[0].city}||${resp.sessions[0].event_date}`;
    await loadTempSession();
  }
}

async function loadTempSession() {
  const sel = document.getElementById("temp-session-select");
  if (!sel.value) return;
  const [city, date] = sel.value.split("||");
  document.getElementById("temp-meta").textContent = "Loading…";

  const d = await fetch(
    `/api/temp-market/session?city=${encodeURIComponent(city)}&date=${encodeURIComponent(date)}`
  ).then(r => r.json());

  if (d.error) {
    document.getElementById("temp-meta").textContent = d.error;
    return;
  }

  const snapCount = d.snapshots.length;
  const obsCount  = d.weather_obs.length;
  const fcCount   = d.forecasts.length;
  document.getElementById("temp-meta").textContent =
    `${snapCount} snapshots  •  ${obsCount} obs  •  ${fcCount} forecasts`;

  renderTempStats(d);
  renderTempBuckets(d);
  renderTempWeather(d);
  renderTempChart(d);
  renderTempPnlChart(d);
}

function renderTempStats(d) {
  const statsEl = document.getElementById("temp-stats");
  const latestC = d.latest_obs_c != null ? d.latest_obs_c.toFixed(1) + "°C" : "—";
  const fcC     = d.latest_forecast_c != null ? d.latest_forecast_c.toFixed(1) + "°C" : "—";
  const best    = d.buckets.find(b => b.condition_id === d.best_bucket_cid);
  const bestLbl = best ? (best.question || "?") : "—";
  const bestMid = best && best.mid != null ? (best.mid * 100).toFixed(1) + "%" : "—";

  statsEl.innerHTML = [
    statCard("City", esc(d.city), ""),
    statCard("Date", esc(d.event_date), ""),
    statCard("Current Obs", latestC, ""),
    statCard("Forecast High", fcC, ""),
    statCard("Best Bucket", esc(bestLbl.length > 22 ? bestLbl.slice(0, 22) + "…" : bestLbl), ""),
    statCard("Best Mid", bestMid, "pos"),
    statCard("Buckets", String(d.buckets.length), ""),
  ].join("");
}

function _bucketLabel(b) {
  if (b.direction === "range" && b.bucket_lo != null && b.bucket_hi != null)
    return `${b.bucket_lo}–${b.bucket_hi}°${b.unit}`;
  if (b.direction === "higher" && b.bucket_lo != null)
    return `≥ ${b.bucket_lo}°${b.unit}`;
  if (b.direction === "below" && b.bucket_lo != null)
    return `≤ ${b.bucket_lo}°${b.unit}`;
  return esc(b.question || "?");
}

function _tempInBucket(temp_c, b) {
  if (temp_c == null) return null;
  const t = b.unit === "F" ? (temp_c * 9 / 5 + 32) : temp_c;
  if (b.direction === "range")
    return t >= b.bucket_lo && t <= b.bucket_hi ? "inside" : t < b.bucket_lo ? "below" : "above";
  if (b.direction === "higher") return t >= b.bucket_lo ? "inside" : "below";
  if (b.direction === "below")  return t <= b.bucket_lo ? "inside" : "above";
  return null;
}

function _vsLabel(rel) {
  if (rel === "inside") return `<span class="badge bg-success">IN</span>`;
  if (rel === "above")  return `<span class="badge bg-warning text-dark">HIGH</span>`;
  if (rel === "below")  return `<span class="badge bg-secondary">LOW</span>`;
  return "—";
}

function renderTempBuckets(d) {
  const tbody = document.getElementById("temp-bucket-body");
  const obs_c  = d.latest_obs_c;
  const fc_c   = d.latest_forecast_c;
  const rows = d.buckets.map((b, i) => {
    const isBest = b.condition_id === d.best_bucket_cid;
    const lbl    = _bucketLabel(b);
    const bid    = b.best_bid  != null ? b.best_bid.toFixed(4)  : "—";
    const ask    = b.best_ask  != null ? b.best_ask.toFixed(4)  : "—";
    const mid    = b.mid       != null ? (b.mid * 100).toFixed(1) + "%" : "—";
    const spr    = b.spread    != null ? b.spread.toFixed(4)    : "—";
    const vsObs  = _vsLabel(_tempInBucket(obs_c, b));
    const vsFc   = _vsLabel(_tempInBucket(fc_c, b));
    const signal = isBest ? `<span class="badge bg-primary">BEST MID</span>` : "";
    const rowCls = isBest ? "table-active" : "";
    const color  = BUCKET_PALETTE[i % BUCKET_PALETTE.length];
    return `<tr class="${rowCls}">
      <td><span style="color:${color};font-weight:600">${lbl}</span></td>
      <td><small class="text-muted">${esc(b.direction)}</small></td>
      <td>${bid}</td><td>${ask}</td><td>${mid}</td><td>${spr}</td>
      <td>${vsObs}</td><td>${vsFc}</td><td>${signal}</td>
    </tr>`;
  });
  tbody.innerHTML = rows.join("") || `<tr><td colspan="9" class="text-muted">No bucket data.</td></tr>`;
}

function renderTempWeather(d) {
  const el = document.getElementById("temp-weather-panel");
  if (!d.weather_obs.length && !d.forecasts.length) {
    el.innerHTML = `<div class="text-muted">No weather data collected yet.</div>`;
    return;
  }

  // Latest per source
  const bySource = {};
  d.weather_obs.forEach(o => { bySource[o.source] = o; });
  const latestFc = d.forecasts.length ? d.forecasts[d.forecasts.length - 1] : null;

  let html = `<table class="table table-sm mb-2"><thead><tr><th>Source</th><th>Time (UTC)</th><th>Temp</th></tr></thead><tbody>`;
  Object.values(bySource).forEach(o => {
    const t = o.temp_c != null ? o.temp_c.toFixed(1) + "°C" : "—";
    html += `<tr><td>${esc(o.source)}</td><td>${(o.observed_at || "").slice(0, 16)}</td><td>${t}</td></tr>`;
  });
  html += `</tbody></table>`;
  if (latestFc) {
    const h = latestFc.forecast_high_c != null ? latestFc.forecast_high_c.toFixed(1) + "°C" : "—";
    html += `<div class="mt-2"><span class="text-muted">Forecast high (${esc(latestFc.source)}):</span> <strong>${h}</strong></div>`;
  }

  // Obs sparkline (text table)
  if (d.weather_obs.length > 1) {
    html += `<div class="mt-3 text-muted small">Recent observations:</div>
    <div style="max-height:160px;overflow-y:auto">
    <table class="table table-sm mb-0"><thead><tr><th>UTC</th><th>°C</th><th>Src</th></tr></thead><tbody>`;
    d.weather_obs.slice(-15).reverse().forEach(o => {
      const t = o.temp_c != null ? o.temp_c.toFixed(1) : "—";
      html += `<tr><td>${(o.observed_at || "").slice(11, 16)}</td><td>${t}</td><td>${esc(o.source)}</td></tr>`;
    });
    html += `</tbody></table></div>`;
  }

  el.innerHTML = html;
}

function renderTempChart(d) {
  // Build time-series per bucket (local_hour on X, mid on Y)
  const bucketMap = {};
  d.buckets.forEach(b => { bucketMap[b.condition_id] = b; });

  // Collect all unique timestamps for X axis
  const tsSet = new Set();
  d.snapshots.forEach(s => tsSet.add(s.sampled_at));
  const allTs = Array.from(tsSet).sort();
  const labels = allTs.map(ts => {
    const s = d.snapshots.find(x => x.sampled_at === ts);
    const lh = s ? s.local_hour : null;
    if (lh == null) return ts.slice(11, 16);
    const h = Math.floor(lh), m = Math.round((lh - h) * 60);
    return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
  });
  // Remove duplicates while preserving order
  const dedupLabels = [...new Set(labels)];

  // Per-bucket series
  const datasets = d.buckets.map((b, i) => {
    const data = allTs.map(ts => {
      const snap = d.snapshots.find(s => s.sampled_at === ts && s.condition_id === b.condition_id);
      return snap && snap.mid != null ? +(snap.mid * 100).toFixed(2) : null;
    });
    const color = BUCKET_PALETTE[i % BUCKET_PALETTE.length];
    const isBest = b.condition_id === d.best_bucket_cid;
    return {
      label: _bucketLabel(b),
      data,
      borderColor: color,
      backgroundColor: color + "22",
      borderWidth: isBest ? 3 : 1.5,
      pointRadius: 2,
      tension: 0.3,
      spanGaps: true,
    };
  });

  if (tempChart) tempChart.destroy();
  const ctx = document.getElementById("temp-chart").getContext("2d");
  tempChart = new Chart(ctx, {
    type: "line",
    data: { labels: allTs.map((ts, i) => {
      const s = d.snapshots.find(x => x.sampled_at === ts);
      const lh = s ? s.local_hour : null;
      if (lh == null) return ts.slice(11, 16);
      const h = Math.floor(lh), m = Math.round((lh - h) * 60);
      return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
    }), datasets },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "right", labels: { boxWidth: 12, font: { size: 11 } } },
      },
      scales: {
        x: { title: { display: true, text: "Local time" } },
        y: { title: { display: true, text: "Mid price (%)" }, min: 0, max: 100 },
      },
    },
  });
}

function renderTempPnlChart(d) {
  // Strategy: for each bucket, enter at first observed ask, exit at latest observed mid.
  // P/L = exit_mid - entry_ask (per 1 share).
  const byBucket = {};
  d.snapshots.forEach(s => {
    if (!byBucket[s.condition_id]) byBucket[s.condition_id] = [];
    byBucket[s.condition_id].push(s);
  });

  // Build cumulative P/L path for the "best bucket at each point in time" strategy
  // Simplified: track the bucket with highest mid at each snapshot time and simulate
  // entry at first seen ask for that bucket.
  const allTs = Array.from(new Set(d.snapshots.map(s => s.sampled_at))).sort();

  const entryAsk = {};  // cid -> first ask seen
  allTs.forEach(ts => {
    d.snapshots.filter(s => s.sampled_at === ts).forEach(s => {
      if (!(s.condition_id in entryAsk) && s.best_ask != null) {
        entryAsk[s.condition_id] = s.best_ask;
      }
    });
  });

  // For each timestamp, compute unrealised P/L of a position entered at first snapshot
  // in the bucket that had highest mid at first timestamp.
  const firstTs = allTs[0];
  if (!firstTs) {
    if (tempPnlChart) { tempPnlChart.destroy(); tempPnlChart = null; }
    return;
  }
  const firstSnaps = d.snapshots.filter(s => s.sampled_at === firstTs);
  if (!firstSnaps.length) return;
  const entryBucket = firstSnaps.reduce((best, s) =>
    (s.mid != null && (best == null || s.mid > best.mid)) ? s : best, null);
  if (!entryBucket) return;

  const entryPrice = entryBucket.best_ask || entryBucket.mid;
  const entryCid   = entryBucket.condition_id;

  const labels = [];
  const pnlSeries = [];

  allTs.forEach(ts => {
    const snap = d.snapshots.find(s => s.sampled_at === ts && s.condition_id === entryCid);
    if (!snap) return;
    const mid = snap.mid;
    if (mid == null) return;
    const pnl = mid - entryPrice;
    const s = d.snapshots.find(x => x.sampled_at === ts && x.condition_id === entryCid);
    const lh = s ? s.local_hour : null;
    if (lh == null) {
      labels.push(ts.slice(11, 16));
    } else {
      const h = Math.floor(lh), m = Math.round((lh - h) * 60);
      labels.push(`${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`);
    }
    pnlSeries.push(+(pnl * 100).toFixed(3));
  });

  const bkt = d.buckets.find(b => b.condition_id === entryCid);
  const bktLbl = bkt ? _bucketLabel(bkt) : entryCid;

  if (tempPnlChart) tempPnlChart.destroy();
  const ctx = document.getElementById("temp-pnl-chart").getContext("2d");
  tempPnlChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: `P/L — ${bktLbl} (entry ask ${entryPrice != null ? (entryPrice * 100).toFixed(1) + "%" : "?"})`,
        data: pnlSeries,
        borderColor: "#60a5fa",
        backgroundColor: "rgba(96,165,250,0.12)",
        borderWidth: 2,
        pointRadius: 2,
        tension: 0.3,
        fill: true,
        spanGaps: true,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { position: "top" } },
      scales: {
        x: { title: { display: true, text: "Local time" } },
        y: { title: { display: true, text: "Unrealised P/L (%)" } },
      },
    },
  });
}
