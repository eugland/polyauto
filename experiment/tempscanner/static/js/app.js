// ── utilities ──────────────────────────────────────────────────────────────────
function esc(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function statBox(label, value, cls = "") {
  return `<div class="stat-box text-center">
    <div class="text-muted small">${label}</div>
    <div class="stat-val ${cls}">${value}</div>
  </div>`;
}
function fmtLocalHour(lh) {
  if (lh == null) return "??:??";
  const h = Math.floor(lh), m = Math.round((lh - h) * 60);
  return `${String(h).padStart(2,"0")}:${String(m).padStart(2,"0")}`;
}

const PALETTE = [
  "#60a5fa","#34d399","#fbbf24","#f87171","#a78bfa",
  "#fb923c","#2dd4bf","#e879f9","#facc15","#4ade80",
  "#38bdf8","#f472b6","#c084fc","#86efac","#fda4af",
  "#67e8f9","#fcd34d","#d8b4fe","#6ee7b7","#fca5a5",
];

// ── state ──────────────────────────────────────────────────────────────────────
let midChart = null, pnlChart = null;
let currentSession = null;

// ── boot ───────────────────────────────────────────────────────────────────────
window.addEventListener("DOMContentLoaded", () => {
  loadCities();
  setInterval(() => {
    if (currentSession) reloadSession();
  }, 60_000);
});

async function loadCities() {
  const d = await fetch("/api/cities").then(r => r.json());
  const sel = document.getElementById("session-select");
  sel.innerHTML = `<option value="">— select city / date —</option>`;
  if (d.error) {
    document.getElementById("session-meta").textContent = d.error;
    return;
  }
  d.sessions.forEach(s => {
    const opt = document.createElement("option");
    opt.value = `${s.city}||${s.event_date}`;
    opt.textContent = `${s.city}  —  ${s.event_date}  (${s.buckets} buckets)`;
    sel.appendChild(opt);
  });
  document.getElementById("session-meta").textContent =
    `${d.sessions.length} session(s)`;
  if (d.sessions.length) {
    sel.value = `${d.sessions[0].city}||${d.sessions[0].event_date}`;
    await loadSession();
  }
}

async function refresh() {
  await loadCities();
  document.getElementById("last-refresh").textContent =
    "refreshed " + new Date().toLocaleTimeString();
}

async function loadSession() {
  const sel = document.getElementById("session-select");
  if (!sel.value) return;
  const [city, date] = sel.value.split("||");
  currentSession = { city, date };
  await fetchAndRender(city, date);
}

async function reloadSession() {
  if (!currentSession) return;
  await fetchAndRender(currentSession.city, currentSession.date);
}

async function fetchAndRender(city, date) {
  document.getElementById("session-meta").textContent = "Loading…";
  const d = await fetch(
    `/api/session?city=${encodeURIComponent(city)}&date=${encodeURIComponent(date)}`
  ).then(r => r.json());

  if (d.error) {
    document.getElementById("session-meta").textContent = d.error;
    return;
  }
  document.getElementById("session-meta").textContent =
    `${d.snapshots.length} snapshots · ${d.weather_obs.length} obs · ${d.forecasts.length} forecasts`;

  renderStats(d);
  renderBuckets(d);
  renderWeather(d);
  renderMidChart(d);
  renderPnlChart(d);
}

// ── stat boxes ─────────────────────────────────────────────────────────────────
function renderStats(d) {
  const best = d.buckets.find(b => b.condition_id === d.best_bucket_cid);
  const obsC  = d.latest_obs_c  != null ? d.latest_obs_c.toFixed(1)  + "°C" : "—";
  const fcC   = d.latest_forecast_c != null ? d.latest_forecast_c.toFixed(1) + "°C" : "—";
  const bestLbl = best ? bucketLabel(best) : "—";
  const bestMid = best?.mid != null ? (best.mid * 100).toFixed(1) + "%" : "—";
  document.getElementById("stat-boxes").innerHTML = [
    statBox("City",          esc(d.city)),
    statBox("Date",          esc(d.event_date)),
    statBox("Observed Temp", obsC),
    statBox("Forecast High", fcC),
    statBox("Best Bucket",   esc(bestLbl.length > 20 ? bestLbl.slice(0,20)+"…" : bestLbl)),
    statBox("Best Mid",      bestMid, "pos"),
    statBox("Buckets",       String(d.buckets.length)),
  ].join("");
}

// ── bucket label helpers ───────────────────────────────────────────────────────
function bucketLabel(b) {
  if (b.direction === "range"  && b.bucket_lo != null && b.bucket_hi != null)
    return `${b.bucket_lo}–${b.bucket_hi}°${b.unit}`;
  if (b.direction === "higher" && b.bucket_lo != null) return `≥ ${b.bucket_lo}°${b.unit}`;
  if (b.direction === "below"  && b.bucket_lo != null) return `≤ ${b.bucket_lo}°${b.unit}`;
  return b.question || "?";
}

function tempInBucket(temp_c, b) {
  if (temp_c == null) return null;
  const t = b.unit === "F" ? (temp_c * 9 / 5 + 32) : temp_c;
  if (b.direction === "range")  return t >= b.bucket_lo && t <= b.bucket_hi ? "in" : t < b.bucket_lo ? "low" : "high";
  if (b.direction === "higher") return t >= b.bucket_lo ? "in" : "low";
  if (b.direction === "below")  return t <= b.bucket_lo ? "in" : "high";
  return null;
}

function relBadge(rel) {
  if (rel === "in")   return `<span class="badge badge-in">IN</span>`;
  if (rel === "high") return `<span class="badge badge-high">HIGH</span>`;
  if (rel === "low")  return `<span class="badge badge-low">LOW</span>`;
  return `<span class="text-muted">—</span>`;
}

// ── bucket table ───────────────────────────────────────────────────────────────
function renderBuckets(d) {
  const tbody = document.getElementById("bucket-body");
  const ts = d.buckets.find(b => b.sampled_at)?.sampled_at;
  document.getElementById("book-ts").textContent = ts ? ts.slice(0,19) + " UTC" : "";

  if (!d.buckets.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="text-muted text-center">No bucket data.</td></tr>`;
    return;
  }
  tbody.innerHTML = d.buckets.map((b, i) => {
    const isBest = b.condition_id === d.best_bucket_cid;
    const color  = PALETTE[i % PALETTE.length];
    const lbl    = bucketLabel(b);
    const bid    = b.best_bid  != null ? b.best_bid.toFixed(4)        : "—";
    const ask    = b.best_ask  != null ? b.best_ask.toFixed(4)        : "—";
    const mid    = b.mid       != null ? (b.mid * 100).toFixed(1)+"%" : "—";
    const spr    = b.spread    != null ? b.spread.toFixed(4)          : "—";
    const vsObs  = relBadge(tempInBucket(d.latest_obs_c, b));
    const vsFc   = relBadge(tempInBucket(d.latest_forecast_c, b));
    const signal = isBest ? `<span class="badge bg-primary">BEST</span>` : "";
    return `<tr${isBest ? ' class="table-active"' : ""}>
      <td><span style="color:${color};font-weight:600">${esc(lbl)}</span></td>
      <td class="text-end">${bid}</td>
      <td class="text-end">${ask}</td>
      <td class="text-end fw-semibold">${mid}</td>
      <td class="text-end text-muted">${spr}</td>
      <td class="text-center">${vsObs}</td>
      <td class="text-center">${vsFc}</td>
      <td class="text-center">${signal}</td>
    </tr>`;
  }).join("");
}

// ── weather panel ──────────────────────────────────────────────────────────────
function renderWeather(d) {
  const el = document.getElementById("weather-panel");
  if (!d.weather_obs.length && !d.forecasts.length) {
    el.innerHTML = `<span class="text-muted small">No weather data collected yet.</span>`;
    return;
  }
  const bySource = {};
  d.weather_obs.forEach(o => { bySource[o.source] = o; });
  const latestFc = d.forecasts.at(-1);

  let html = `<table class="table table-sm mb-2">
    <thead><tr><th>Source</th><th>Time</th><th>Temp</th></tr></thead><tbody>`;
  Object.values(bySource).forEach(o => {
    const t = o.temp_c != null ? o.temp_c.toFixed(1)+"°C" : "—";
    html += `<tr><td>${esc(o.source)}</td><td>${(o.observed_at||"").slice(11,16)}</td><td>${t}</td></tr>`;
  });
  html += `</tbody></table>`;
  if (latestFc) {
    const h = latestFc.forecast_high_c != null ? latestFc.forecast_high_c.toFixed(1)+"°C" : "—";
    html += `<div class="mt-1 small"><span class="text-muted">Forecast high (${esc(latestFc.source)}):</span>
      <strong>${h}</strong></div>`;
  }
  if (d.weather_obs.length > 1) {
    html += `<div class="mt-3 text-muted small mb-1">History (latest first):</div>
    <div style="max-height:150px;overflow-y:auto">
    <table class="table table-sm mb-0"><thead><tr><th>UTC</th><th>°C</th><th>Src</th></tr></thead><tbody>`;
    d.weather_obs.slice().reverse().slice(0, 20).forEach(o => {
      const t = o.temp_c != null ? o.temp_c.toFixed(1) : "—";
      html += `<tr><td>${(o.observed_at||"").slice(11,16)}</td><td>${t}</td><td>${esc(o.source)}</td></tr>`;
    });
    html += `</tbody></table></div>`;
  }
  el.innerHTML = html;
}

// ── mid-price chart ────────────────────────────────────────────────────────────
function renderMidChart(d) {
  const allTs = [...new Set(d.snapshots.map(s => s.sampled_at))].sort();
  if (!allTs.length) return;

  const labels = allTs.map(ts => {
    const s = d.snapshots.find(x => x.sampled_at === ts);
    return fmtLocalHour(s?.local_hour);
  });

  const datasets = d.buckets.map((b, i) => {
    const color  = PALETTE[i % PALETTE.length];
    const isBest = b.condition_id === d.best_bucket_cid;
    const data   = allTs.map(ts => {
      const s = d.snapshots.find(x => x.sampled_at === ts && x.condition_id === b.condition_id);
      return s?.mid != null ? +(s.mid * 100).toFixed(2) : null;
    });
    return {
      label: bucketLabel(b), data,
      borderColor: color, backgroundColor: color + "22",
      borderWidth: isBest ? 3 : 1.5,
      pointRadius: isBest ? 3 : 1.5,
      tension: 0.3, spanGaps: true,
    };
  });

  if (midChart) midChart.destroy();
  midChart = new Chart(document.getElementById("mid-chart").getContext("2d"), {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "right", labels: { boxWidth: 12, font: { size: 11 } } } },
      scales: {
        x: { title: { display: true, text: "Local time" } },
        y: { title: { display: true, text: "Mid price (%)" }, min: 0, max: 100 },
      },
    },
  });
}

// ── P/L simulation chart ───────────────────────────────────────────────────────
function renderPnlChart(d) {
  const allTs = [...new Set(d.snapshots.map(s => s.sampled_at))].sort();
  if (allTs.length < 2) {
    if (pnlChart) { pnlChart.destroy(); pnlChart = null; }
    return;
  }

  // Pick entry bucket: highest mid at first timestamp
  const firstTs = allTs[0];
  const firstSnaps = d.snapshots.filter(s => s.sampled_at === firstTs);
  const entry = firstSnaps.reduce((best, s) =>
    s.mid != null && (best == null || s.mid > best.mid) ? s : best, null);
  if (!entry) return;

  const entryPrice = entry.best_ask ?? entry.mid;
  const cid        = entry.condition_id;
  const bkt        = d.buckets.find(b => b.condition_id === cid);

  const labels = [], pnlData = [];
  allTs.forEach(ts => {
    const s = d.snapshots.find(x => x.sampled_at === ts && x.condition_id === cid);
    if (!s || s.mid == null) return;
    labels.push(fmtLocalHour(s.local_hour));
    pnlData.push(+((s.mid - entryPrice) * 100).toFixed(3));
  });

  const entryLbl = bkt ? bucketLabel(bkt) : cid.slice(0, 12) + "…";
  const entryPct = entryPrice != null ? (entryPrice * 100).toFixed(1) + "%" : "?";

  if (pnlChart) pnlChart.destroy();
  pnlChart = new Chart(document.getElementById("pnl-chart").getContext("2d"), {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: `${entryLbl}  (entry ask ${entryPct})`,
        data: pnlData,
        borderColor: "#60a5fa",
        backgroundColor: "rgba(96,165,250,0.12)",
        borderWidth: 2, pointRadius: 2, tension: 0.3, fill: true, spanGaps: true,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { position: "top" } },
      scales: {
        x: { title: { display: true, text: "Local time" } },
        y: { title: { display: true, text: "Unrealised P/L (cents per $1 share)" } },
      },
    },
  });
}
