// ── utilities ─────────────────────────────────────────────────────────────
function esc(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function fmtPct(p, digits = 2) {
  if (p == null || Number.isNaN(p)) return "—";
  const pct = p * 100;
  const cls = pct > 0 ? "pos" : pct < 0 ? "neg" : "neu";
  const sign = pct > 0 ? "+" : "";
  return `<span class="${cls}">${sign}${pct.toFixed(digits)}%</span>`;
}
function fmtNum(v, digits = 2) {
  if (v == null || Number.isNaN(v)) return "—";
  return Number(v).toFixed(digits);
}
function fmtCompactVol(v) {
  if (v == null) return "—";
  if (v >= 1e9) return (v / 1e9).toFixed(1) + "B";
  if (v >= 1e6) return (v / 1e6).toFixed(1) + "M";
  if (v >= 1e3) return (v / 1e3).toFixed(1) + "K";
  return String(v);
}
function classifyPct(p) {
  if (p == null) return "neu";
  return p > 0 ? "pos" : p < 0 ? "neg" : "neu";
}

const PALETTE = [
  "#60a5fa","#34d399","#fbbf24","#f87171","#a78bfa",
  "#fb923c","#2dd4bf","#e879f9","#facc15","#4ade80",
  "#38bdf8","#f472b6","#c084fc","#86efac","#fda4af",
];

// Force light mode and chart text/grid styling.
document.documentElement.setAttribute("data-bs-theme", "light");
if (window.Chart) {
  Chart.defaults.color = "#1f2937";
  Chart.defaults.borderColor = "rgba(31, 41, 55, 0.16)";
}

const SECTOR_COLOR = {
  "Technology":             "#60a5fa",
  "Financials":             "#34d399",
  "Health Care":            "#f87171",
  "Consumer Discretionary": "#a78bfa",
  "Consumer Staples":       "#fb923c",
  "Communication Services": "#2dd4bf",
  "Energy":                 "#facc15",
  "Industrials":            "#e879f9",
  "Materials":              "#fbbf24",
  "Utilities":              "#38bdf8",
  "Real Estate":            "#f472b6",
  "Volatility":             "#c084fc",
};
function sectorColor(s) { return SECTOR_COLOR[s] || "#9ca3af"; }

// ── chart registry ─────────────────────────────────────────────────────────
const charts = {};
function setChart(id, cfg) {
  if (charts[id]) charts[id].destroy();
  charts[id] = new Chart(document.getElementById(id).getContext("2d"), cfg);
}

function withBasePath(url) {
  if (!url || !url.startsWith("/")) return url;
  // When served behind nginx at /stock/, keep API calls under that prefix.
  const p = window.location.pathname || "/";
  if (p === "/stock" || p.startsWith("/stock/")) return "/stock" + url;
  return url;
}

// ── fetch helper ──────────────────────────────────────────────────────────
async function fetchJSON(url) {
  try {
    const r = await fetch(withBasePath(url));
    if (!r.ok) throw new Error(r.status);
    return await r.json();
  } catch (e) {
    console.warn("fetch failed", url, e);
    return null;
  }
}

// ── state ─────────────────────────────────────────────────────────────────
const state = {
  svix_view: "5d",
  tsl_view: "5d",
  spy_obv_view: "current",
  tsl_drift_view: "current",
  div_profile: "default",
};

// ── boot ──────────────────────────────────────────────────────────────────
window.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("button[data-pair]").forEach(btn => {
    btn.addEventListener("click", () => {
      const pair = btn.dataset.pair, view = btn.dataset.view;
      document.querySelectorAll(`button[data-pair="${pair}"]`)
        .forEach(b => b.classList.toggle("active", b === btn));
      state[`${pair}_view`] = view;
      if (pair === "svix") renderSvix();
      else renderTsl();
    });
  });
  document.querySelectorAll("button[data-obv-view]").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("button[data-obv-view]")
        .forEach(b => b.classList.toggle("active", b === btn));
      state.spy_obv_view = btn.dataset.obvView || "current";
      refreshSpyVolumeSignal();
    });
  });
  document.querySelectorAll("button[data-drift-view]").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("button[data-drift-view]")
        .forEach(b => b.classList.toggle("active", b === btn));
      state.tsl_drift_view = btn.dataset.driftView || "current";
      refreshTslDriftChart();
    });
  });
  document.getElementById("apply-spy-obv-range")?.addEventListener("click", refreshSpyVolumeSignal);
  document.getElementById("apply-tsl-drift-range")?.addEventListener("click", refreshTslDriftChart);
  document.getElementById("div-load-profile")?.addEventListener("click", loadSelectedDividendProfile);
  document.getElementById("div-save-profile")?.addEventListener("click", saveDividendProfile);
  document.getElementById("div-duplicate-profile")?.addEventListener("click", duplicateDividendProfile);
  document.getElementById("div-delete-profile")?.addEventListener("click", deleteDividendProfile);
  refreshAll();
  setInterval(refreshQuotes, 30_000);
  setInterval(refreshSlow,   5 * 60_000);
});

async function refreshAll() {
  await Promise.all([
    refreshHealth(), refreshEcon(), refreshMacro(), refreshBreadth(),
    refreshVixTerm(), refreshVvix(),
    refreshFearGreed(), refreshSpyVolumeSignal(),
    renderSvix(), renderTsl(),
    refreshTslDriftChart(),
    refreshSectors(), refreshCorr(), refreshVolScatter(), refreshBubble(),
    refreshMovers(), refreshScreeners(), refreshEarnings(),
    refreshOvernight(), refreshImpliedOpen(), refreshPremarketGappers(),
    initDividendSection(),
  ]);
  document.getElementById("last-refresh").textContent =
    "last refresh " + new Date().toLocaleTimeString();
}

async function refreshQuotes() {
  refreshMacro(); refreshBreadth(); refreshSectors();
  refreshBubble(); refreshMovers(); refreshScreeners();
  refreshVixTerm(); refreshFearGreed(); refreshSpyVolumeSignal();
  refreshOvernight(); refreshImpliedOpen();
  document.getElementById("last-refresh").textContent =
    "last refresh " + new Date().toLocaleTimeString();
}

async function refreshSlow() {
  refreshCorr(); refreshVolScatter();
  refreshTslDriftChart();
  refreshEarnings(); refreshEcon();
  refreshPremarketGappers();
}

async function initDividendSection() {
  await refreshDividendProfiles();
  await loadDividendProfile(state.div_profile);
  await refreshDividendReport();
}

// ── Health ─────────────────────────────────────────────────────────────────
async function refreshHealth() {
  const d = await fetchJSON("/api/health");
  if (!d) return;
  const meta = `DB: ${d.db} · ${d.quotes ?? 0} quotes · ${d.daily_bars ?? 0} daily bars`
              + (d.last_poll ? ` · last ${d.last_poll.slice(11,19)} UTC` : "");
  document.getElementById("health-meta").textContent = meta;
}

// ── Econ banner ────────────────────────────────────────────────────────────
async function refreshEcon() {
  const d = await fetchJSON("/api/econ-events?days=1");
  const el = document.getElementById("econ-banner");
  if (!d || !d.length) { el.style.display = "none"; return; }
  const parts = d.slice(0, 6).map(e =>
    `<span class="me-3"><strong>${esc(e.event_time)}</strong> ${esc(e.name)}`
    + (e.forecast ? ` (est ${esc(e.forecast)})` : "")
    + (e.actual   ? ` → <strong>${esc(e.actual)}</strong>` : "") + "</span>");
  el.innerHTML = `<strong>Today:</strong> ${parts.join("")}`;
  el.style.display = "block";
}

// ── Macro tiles ────────────────────────────────────────────────────────────
async function refreshMacro() {
  const d = await fetchJSON("/api/macro");
  const el = document.getElementById("macro-tiles");
  if (!d || !d.length) { el.innerHTML = `<span class="text-muted small">no data</span>`; return; }
  el.innerHTML = d.map(m => {
    const cls = classifyPct(m.pct);
    const bg = m.pct > 0 ? "bg-pos" : m.pct < 0 ? "bg-neg" : "";
    return `<div class="macro-tile ${bg}">
      <div class="label">${esc(m.symbol)}</div>
      <div class="val">${fmtNum(m.last, 2)}</div>
      <div class="pct ${cls}">${fmtPct(m.pct)}</div>
    </div>`;
  }).join("");
}

// ── Overnight markets ─────────────────────────────────────────────────────
async function refreshOvernight() {
  const d = await fetchJSON("/api/overnight");
  const el = document.getElementById("overnight-tiles");
  const meta = document.getElementById("overnight-meta");
  if (!el) return;
  if (!d || !d.data || !d.data.length) {
    el.innerHTML = `<span class="text-muted small">no data</span>`;
    if (meta) meta.textContent = "";
    return;
  }
  if (meta && d.as_of) {
    meta.textContent = `as of ${new Date(d.as_of * 1000).toLocaleTimeString()}`
      + (d.cached ? " (cached)" : "");
  }
  el.innerHTML = d.data.map(m => {
    if (m.error) {
      return `<div class="macro-tile">
        <div class="label">${esc(m.label || m.symbol)}</div>
        <div class="val text-muted">—</div>
        <div class="pct text-muted small">err</div>
      </div>`;
    }
    const cls = classifyPct(m.pct);
    const bg  = m.pct > 0 ? "bg-pos" : m.pct < 0 ? "bg-neg" : "";
    const digits = (m.symbol === "BTC-USD" || m.symbol === "ETH-USD") ? 0
                : (m.symbol === "^TNX" ? 3 : 2);
    return `<div class="macro-tile ${bg}" title="${esc(m.symbol)} prev close ${fmtNum(m.prev_close, digits)}">
      <div class="label">${esc(m.label || m.symbol)}</div>
      <div class="val">${fmtNum(m.last, digits)}</div>
      <div class="pct ${cls}">${fmtPct(m.pct)}</div>
    </div>`;
  }).join("");
}

// ── Futures-implied cash open ─────────────────────────────────────────────
function fmtIndexPrice(v) {
  if (v == null || Number.isNaN(v)) return "—";
  return Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
function fmtPts(v) {
  if (v == null || Number.isNaN(v)) return "—";
  const n = Number(v);
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(1)} pts`;
}
async function refreshImpliedOpen() {
  const d = await fetchJSON("/api/implied-open");
  const el = document.getElementById("implied-open-tiles");
  const meta = document.getElementById("implied-open-meta");
  if (!el) return;
  if (!d || !d.data || !d.data.length) {
    el.innerHTML = `<span class="text-muted small">no data</span>`;
    if (meta) meta.textContent = "";
    return;
  }
  if (meta && d.as_of) {
    meta.textContent = `as of ${new Date(d.as_of * 1000).toLocaleTimeString()}`
      + (d.cached ? " (cached)" : "");
  }
  el.innerHTML = d.data.map(r => {
    const cls = classifyPct(r.delta_pct);
    const arrow = r.delta_pct == null ? "" : (r.delta_pct > 0 ? "▲" : r.delta_pct < 0 ? "▼" : "—");
    const bg = r.delta_pct > 0 ? "bg-pos" : r.delta_pct < 0 ? "bg-neg" : "";
    return `<div class="col-md-6 col-xl-3">
      <div class="implied-tile ${bg}" style="border:1px solid var(--bs-border-color); border-radius:6px; padding:10px;">
        <div class="d-flex justify-content-between align-items-baseline">
          <strong>${esc(r.label)}</strong>
          <small class="text-muted">${esc(r.futures)} → ${esc(r.cash)}</small>
        </div>
        <div class="${cls}" style="font-size:1.4rem; font-weight:600; line-height:1.2;">
          ${arrow} ${fmtIndexPrice(r.implied_open)}
        </div>
        <div class="${cls} small">${fmtPts(r.delta_pts)} (${fmtPct(r.delta_pct)})</div>
        <div class="text-muted small mt-1">prev close ${fmtIndexPrice(r.cash_prev_close)}</div>
      </div>
    </div>`;
  }).join("");
}

// ── Pre-market gappers (with overnight-trend fallback) ───────────────────
async function refreshPremarketGappers() {
  const d = await fetchJSON("/api/premarket-gappers");
  const body = document.getElementById("premkt-body");
  const meta = document.getElementById("premkt-meta");
  const title = document.getElementById("premkt-title");
  const tableWrap = document.getElementById("premkt-table-wrap");
  const chartWrap = document.getElementById("premkt-chart-wrap");
  if (!body) return;

  const hasData = d && d.data && d.data.length;
  if (hasData) {
    if (title) title.textContent = "Pre-Market Gappers (S&P 500, top by avg volume)";
    if (tableWrap) tableWrap.style.display = "";
    if (chartWrap) chartWrap.style.display = "none";
    if (meta && d.as_of) {
      meta.textContent = `${d.data.length} names · as of ${new Date(d.as_of * 1000).toLocaleTimeString()}`
        + (d.cached ? " (cached)" : "");
    }
    body.innerHTML = d.data.map(r => `
      <tr>
        <td><strong>${esc(r.symbol)}</strong></td>
        <td class="text-truncate" style="max-width:180px">${esc(r.name || "")}</td>
        <td><small style="color:${sectorColor(r.sector)}">${esc(r.sector || "")}</small></td>
        <td class="text-end">${fmtNum(r.last)}</td>
        <td class="text-end text-muted">${fmtNum(r.prev_close)}</td>
        <td class="text-end">${fmtPct(r.premarket_pct)}</td>
        <td class="text-end">${fmtCompactVol(r.premarket_volume)}</td>
      </tr>`).join("");
    return;
  }
  // Fallback: show overnight-trend chart instead.
  if (title) title.textContent = "Overnight Futures & Crypto (last ~48h, % from start)";
  if (tableWrap) tableWrap.style.display = "none";
  if (chartWrap) chartWrap.style.display = "";
  await refreshOvernightTrend();
}

const _OVERNIGHT_TREND_COLORS = {
  "ES=F":    "#60a5fa",
  "NQ=F":    "#a78bfa",
  "BTC-USD": "#fbbf24",
  "ETH-USD": "#34d399",
};
async function refreshOvernightTrend() {
  const d = await fetchJSON("/api/overnight-trend");
  const meta = document.getElementById("premkt-meta");
  if (!d || !d.data || !d.data.length) {
    if (meta) meta.textContent = "no overnight data";
    return;
  }
  if (meta && d.as_of) {
    meta.textContent = `as of ${new Date(d.as_of * 1000).toLocaleTimeString()}`
      + (d.cached ? " (cached)" : "");
  }
  // Use the longest series as the canonical x-axis label set, then align
  // shorter series by their own timestamps (category match by string).
  const longest = d.data.reduce((a, b) => (b.points.length > a.points.length ? b : a), d.data[0]);
  const labels = longest.points.map(p => p.t);
  const datasets = d.data.map(s => {
    const map = new Map(s.points.map(p => [p.t, p.value * 100]));
    return {
      label: s.label,
      data: labels.map(t => (map.has(t) ? map.get(t) : null)),
      borderColor: _OVERNIGHT_TREND_COLORS[s.symbol] || "#9ca3af",
      backgroundColor: _OVERNIGHT_TREND_COLORS[s.symbol] || "#9ca3af",
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.15,
      spanGaps: true,
    };
  });
  setChart("overnight-trend-chart", {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "top" },
        tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y == null ? "—" : ctx.parsed.y.toFixed(2) + "%"}` } },
      },
      scales: {
        x: { ticks: { maxTicksLimit: 10, maxRotation: 0, minRotation: 0, font: { size: 10 } } },
        y: { ticks: { callback: v => v.toFixed(1) + "%" }, title: { display: true, text: "% from start" } },
      },
    },
  });
}

// ── Breadth ────────────────────────────────────────────────────────────────
async function refreshBreadth() {
  const d = await fetchJSON("/api/breadth");
  if (!d) return;
  setChart("breadth-chart", {
    type: "bar",
    data: {
      labels: ["Advancers / Decliners", "52w Highs / Lows"],
      datasets: [
        { label: "Positive", data: [d.advancers, d.new_highs], backgroundColor: "#4ade80", stack: "s" },
        { label: "Negative", data: [d.decliners, d.new_lows],  backgroundColor: "#f87171", stack: "s" },
      ],
    },
    options: {
      indexAxis: "y", responsive: true,
      plugins: { legend: { display: false } },
      scales: { x: { stacked: true }, y: { stacked: true } },
    },
  });
  const total = (d.advancers + d.decliners) || 1;
  const ratio = (d.advancers / total * 100).toFixed(0);
  document.getElementById("breadth-meta").innerHTML =
    `${d.advancers} up · ${d.decliners} down · ${d.unchanged} flat <br>
     ${d.new_highs} new 52w highs · ${d.new_lows} new 52w lows
     <span class="ms-2">(A/D ratio ${ratio}%)</span>`;
}

// ── VIX term ───────────────────────────────────────────────────────────────
async function refreshVixTerm() {
  const d = await fetchJSON("/api/vix-term");
  if (!d) return;
  const latest = d.latest || {};
  const datasets = [
    { label: "VIX", data: [latest["^VIX"]] },
    { label: "VIX3M", data: [latest["^VIX3M"]] },
    { label: "VIX6M", data: [latest["^VIX6M"]] },
  ].map((ds, i) => ({
    ...ds, backgroundColor: PALETTE[i], borderColor: PALETTE[i],
  }));
  setChart("vix-term-chart", {
    type: "bar",
    data: { labels: ["VIX", "VIX3M", "VIX6M"],
            datasets: [{
              data: [latest["^VIX"], latest["^VIX3M"], latest["^VIX6M"]],
              backgroundColor: PALETTE.slice(0,3),
            }]},
    options: {
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true } },
    },
  });
  const ratio = d.contango_ratio;
  if (ratio != null) {
    const label = ratio > 1 ? "contango" : "backwardation";
    const cls = ratio > 1 ? "pos" : "neg";
    document.getElementById("vix-contango").innerHTML =
      `<span class="${cls}">${ratio.toFixed(2)}x ${label}</span>`;
  }
}

// ── VVIX mini ─────────────────────────────────────────────────────────────
async function refreshVvix() {
  const d = await fetchJSON("/api/vvix");
  if (!d || !d.points || !d.points.length) return;
  setChart("vvix-chart", {
    type: "line",
    data: {
      labels: d.points.map(p => p.t.slice(11, 16)),
      datasets: [{ label: "VVIX", data: d.points.map(p => p.value),
                   borderColor: "#c084fc", backgroundColor: "rgba(192,132,252,.15)",
                   pointRadius: 0, tension: 0.3, borderWidth: 1.5 }]
    },
    options: {
      plugins: { legend: { display: false } },
      scales: { x: { display: false }, y: { display: true, ticks: { font: { size: 9 } } } },
    },
  });
}

// ── pair charts ───────────────────────────────────────────────────────────
async function renderPairCanvas(canvasId, symbols, view) {
  const d = await fetchJSON(`/api/pair?symbols=${symbols.join(",")}&view=${view}`);
  if (!d || !d.series) return;
  const labels = (d.series[0]?.points || []).map(p => p.t);
  const datasets = d.series.map((s, i) => ({
    label: s.symbol,
    data: s.points.map(p => p.value),
    borderColor: PALETTE[i],
    backgroundColor: PALETTE[i] + "22",
    pointRadius: view === "intraday" ? 0 : 2,
    tension: 0.25, borderWidth: 2, spanGaps: true,
  }));
  setChart(canvasId, {
    type: "line",
    data: { labels: labels.map(t => view === "intraday" ? t.slice(11, 16) : t.slice(0, 10)),
            datasets },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "top" } },
      scales: {
        x: { ticks: { maxTicksLimit: 8, font: { size: 10 } } },
        y: { title: { display: true, text: "rebased to 100" } },
      },
    },
  });
}

async function renderSvix() {
  await renderPairCanvas("svix-chart", ["SVIX", "UVIX"], state.svix_view);
}
async function renderTsl() {
  await renderPairCanvas("tsl-chart", ["TSLL", "TSLZ"], state.tsl_view);
}

// ── tracking error ─────────────────────────────────────────────────────────
function buildRangeQS(view, startId, endId) {
  const qs = new URLSearchParams();
  qs.set("view", view || "current");
  const start = document.getElementById(startId)?.value;
  const end = document.getElementById(endId)?.value;
  if (start) qs.set("start", start);
  if (end) qs.set("end", end);
  return qs.toString();
}

function alignSeries(labels, points) {
  const byTs = {};
  (points || []).forEach(p => { byTs[p.t] = p.value; });
  return labels.map(t => byTs[t] ?? null);
}

function labelForView(ts, view) {
  if (!ts) return "";
  if (view === "intraday") return ts.slice(11, 16);
  return ts.slice(0, 10);
}

async function refreshTslDriftChart() {
  const qs = buildRangeQS(state.tsl_drift_view, "tsl-drift-start", "tsl-drift-end");
  const [tsll, tslz] = await Promise.all([
    fetchJSON(`/api/tracking-error?pair=TSLL&base=TSLA&leverage=2&${qs}`),
    fetchJSON(`/api/tracking-error?pair=TSLZ&base=TSLA&leverage=-1&${qs}`),
  ]);
  if (!tsll || !tslz) return;

  const labelsSet = new Set([
    ...(tsll.actual || []).map(p => p.t),
    ...(tsll.synthetic || []).map(p => p.t),
    ...(tslz.actual || []).map(p => p.t),
    ...(tslz.synthetic || []).map(p => p.t),
  ]);
  const labels = Array.from(labelsSet).sort();
  if (!labels.length) {
    document.getElementById("tsll-te").textContent = "TSLL drift: —";
    document.getElementById("tslz-te").textContent = "TSLZ drift: —";
    return;
  }

  setChart("tsl-drift-chart", {
    type: "line",
    data: {
      labels: labels.map(t => labelForView(t, state.tsl_drift_view)),
      datasets: [
        {
          label: "TSLL",
          data: alignSeries(labels, tsll.actual),
          borderColor: "#60a5fa",
          backgroundColor: "#60a5fa22",
          tension: 0.25,
          borderWidth: 2,
          pointRadius: state.tsl_drift_view === "intraday" ? 0 : 1.5,
        },
        {
          label: "synthetic 2x TSLA",
          data: alignSeries(labels, tsll.synthetic),
          borderColor: "#93c5fd",
          borderDash: [6, 4],
          tension: 0.25,
          borderWidth: 1.8,
          pointRadius: 0,
        },
        {
          label: "TSLZ",
          data: alignSeries(labels, tslz.actual),
          borderColor: "#f97316",
          backgroundColor: "#f9731622",
          tension: 0.25,
          borderWidth: 2,
          pointRadius: state.tsl_drift_view === "intraday" ? 0 : 1.5,
        },
        {
          label: "synthetic -1x TSLA",
          data: alignSeries(labels, tslz.synthetic),
          borderColor: "#fdba74",
          borderDash: [6, 4],
          tension: 0.25,
          borderWidth: 1.8,
          pointRadius: 0,
        },
      ],
    },
    options: {
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { position: "top" } },
      scales: { y: { title: { display: true, text: "rebased to 100" } } },
    },
  });

  if (tsll.tracking_error_pct != null) {
    const cls = tsll.tracking_error_pct < 0 ? "neg" : "pos";
    document.getElementById("tsll-te").innerHTML =
      `TSLL drift: <span class="${cls}">${tsll.tracking_error_pct.toFixed(2)}pts</span>`;
  } else {
    document.getElementById("tsll-te").textContent = "TSLL drift: —";
  }
  if (tslz.tracking_error_pct != null) {
    const cls = tslz.tracking_error_pct < 0 ? "neg" : "pos";
    document.getElementById("tslz-te").innerHTML =
      `TSLZ drift: <span class="${cls}">${tslz.tracking_error_pct.toFixed(2)}pts</span>`;
  } else {
    document.getElementById("tslz-te").textContent = "TSLZ drift: —";
  }
}

// ── Sectors ───────────────────────────────────────────────────────────────
async function refreshSectors() {
  const d = await fetchJSON("/api/sectors");
  if (!d) return;
  const labels = d.map(s => s.etf);
  const pcts = d.map(s => (s.pct ?? 0) * 100);
  setChart("sector-bar", {
    type: "bar",
    data: { labels, datasets: [{
      data: pcts,
      backgroundColor: pcts.map(p => p >= 0 ? "#4ade80" : "#f87171"),
    }]},
    options: {
      plugins: { legend: { display: false } },
      scales: { y: { title: { display: true, text: "% change" } } },
    },
  });
  setChart("sector-bubble", {
    type: "bubble",
    data: {
      datasets: d.map(s => ({
        label: s.etf,
        data: [{ x: s.price, y: (s.pct ?? 0) * 100,
                 r: Math.max(4, Math.log10((s.volume || 1000) + 1) * 2) }],
        backgroundColor: sectorColor(s.name?.replace(" SPDR", "")) + "cc",
      })),
    },
    options: {
      plugins: { legend: { display: false },
                 tooltip: { callbacks: {
                   label: ctx => {
                     const ds = ctx.dataset; const pt = ctx.raw;
                     return `${ds.label}  $${pt.x?.toFixed(2)}  ${pt.y?.toFixed(2)}%`;
                   }
                 }}},
      scales: { x: { title: { display: true, text: "price" } },
                y: { title: { display: true, text: "% change" } } },
    },
  });
}

// ── Correlation heatmap ───────────────────────────────────────────────────
async function refreshCorr() {
  const d = await fetchJSON("/api/correlation?window=20");
  if (!d || !d.symbols || !d.symbols.length) return;
  const data = [];
  for (let i = 0; i < d.symbols.length; i++) {
    for (let j = 0; j < d.symbols.length; j++) {
      data.push({ x: d.symbols[j], y: d.symbols[i], v: (d.matrix[i] || [])[j] });
    }
  }
  setChart("corr-chart", {
    type: "matrix",
    data: {
      datasets: [{
        label: "20d correlation",
        data,
        backgroundColor(ctx) {
          const v = ctx.raw?.v;
          if (v == null) return "rgba(120,120,120,.2)";
          const r = v > 0 ? Math.round(74 + (255 - 74) * (1 - v))  : 255;
          const g = v > 0 ? Math.round(222 * v + (1 - v) * 200)    : Math.round(200 * (1 + v));
          const b = v > 0 ? Math.round(128 * v + (1 - v) * 200)    : Math.round(200 * (1 + v));
          const alpha = 0.3 + Math.abs(v) * 0.7;
          return `rgba(${r},${g},${b},${alpha})`;
        },
        borderWidth: 1, borderColor: "rgba(0,0,0,0.2)",
        width: (ctx) => (ctx.chart.chartArea || {}).width / d.symbols.length - 1,
        height: (ctx) => (ctx.chart.chartArea || {}).height / d.symbols.length - 1,
      }],
    },
    options: {
      plugins: { legend: { display: false },
                 tooltip: { callbacks: {
                   title: () => "",
                   label: ctx => `${ctx.raw.y} × ${ctx.raw.x}: ${(ctx.raw.v ?? 0).toFixed(2)}`,
                 }}},
      scales: {
        x: { type: "category", labels: d.symbols, offset: true,
             ticks: { font: { size: 10 } }, grid: { display: false } },
        y: { type: "category", labels: d.symbols, offset: true,
             ticks: { font: { size: 10 } }, grid: { display: false } },
      },
    },
  });
}

// ── vol scatter ───────────────────────────────────────────────────────────
async function refreshVolScatter() {
  const d = await fetchJSON("/api/vol-scatter");
  if (!d) return;
  const bySector = {};
  d.forEach(r => {
    if (!bySector[r.sector]) bySector[r.sector] = [];
    bySector[r.sector].push({ x: r.volume_ratio, y: r.pct_change * 100, sym: r.symbol });
  });
  setChart("vol-scatter", {
    type: "scatter",
    data: {
      datasets: Object.entries(bySector).map(([sec, pts]) => ({
        label: sec, data: pts,
        backgroundColor: sectorColor(sec) + "bb",
        pointRadius: 3,
      })),
    },
    options: {
      plugins: { legend: { position: "right", labels: { boxWidth: 10, font: { size: 10 } } },
                 tooltip: { callbacks: {
                   label: ctx => `${ctx.raw.sym}  vol ${ctx.raw.x?.toFixed(2)}x  ${ctx.raw.y?.toFixed(2)}%`,
                 }}},
      scales: {
        x: { title: { display: true, text: "volume / 20d avg" }, type: "logarithmic" },
        y: { title: { display: true, text: "% change" } },
      },
    },
  });
}

// ── S&P 500 bubble ────────────────────────────────────────────────────────
async function refreshBubble() {
  const d = await fetchJSON("/api/bubble");
  if (!d) return;
  const bySector = {};
  d.forEach(r => {
    if (!bySector[r.sector]) bySector[r.sector] = [];
    bySector[r.sector].push({
      x: r.price, y: (r.pct ?? 0) * 100,
      r: Math.max(3, Math.log10((r.volume || 1000) + 1) * 1.5),
      sym: r.symbol,
    });
  });
  setChart("sp-bubble", {
    type: "bubble",
    data: {
      datasets: Object.entries(bySector).map(([sec, pts]) => ({
        label: sec, data: pts,
        backgroundColor: sectorColor(sec) + "99",
      })),
    },
    options: {
      plugins: { legend: { position: "right", labels: { boxWidth: 10, font: { size: 10 } } },
                 tooltip: { callbacks: {
                   label: ctx => `${ctx.raw.sym}  $${ctx.raw.x?.toFixed(2)}  ${ctx.raw.y?.toFixed(2)}%`,
                 }}},
      scales: {
        x: { title: { display: true, text: "price" }, type: "logarithmic" },
        y: { title: { display: true, text: "% change" } },
      },
    },
  });
}

// ── movers ────────────────────────────────────────────────────────────────
async function refreshMovers() {
  const [up, down] = await Promise.all([
    fetchJSON("/api/movers?side=up&limit=10"),
    fetchJSON("/api/movers?side=down&limit=10"),
  ]);
  renderMoverTable("movers-up", up || []);
  renderMoverTable("movers-down", down || []);
}

function renderMoverTable(id, rows) {
  const el = document.getElementById(id);
  if (!rows.length) { el.innerHTML =
    `<tr><td colspan="5" class="text-muted text-center py-3">no data</td></tr>`; return; }
  el.innerHTML = rows.map(r => `
    <tr>
      <td><strong>${esc(r.symbol)}</strong></td>
      <td class="text-truncate" style="max-width:180px">${esc(r.name || "")}</td>
      <td><small style="color:${sectorColor(r.sector)}">${esc(r.sector || "")}</small></td>
      <td class="text-end">${fmtNum(r.price)}</td>
      <td class="text-end">${fmtPct(r.pct)}</td>
    </tr>`).join("");
}

// ── screeners ─────────────────────────────────────────────────────────────
async function refreshScreeners() {
  const kinds = ["volume","gap-up","gap-down","near-high","near-low","prepost"];
  for (const k of kinds) {
    const d = await fetchJSON(`/api/screener?kind=${k}`);
    renderScreenerTable(`screener-${k}`, d || [], k);
  }
}

function renderScreenerTable(id, rows, kind) {
  const el = document.getElementById(id);
  if (!rows.length) { el.innerHTML =
    `<div class="text-muted small text-center py-3">no matches</div>`; return; }
  const extraCol = {
    "volume":    {h: "Vol Ratio",     f: r => fmtNum(r.volume_ratio) + "x"},
    "gap-up":    {h: "Gap %",         f: r => fmtPct(r.gap_pct)},
    "gap-down":  {h: "Gap %",         f: r => fmtPct(r.gap_pct)},
    "near-high": {h: "% from 52w Hi", f: r => fmtPct(r.pct_from_high)},
    "near-low":  {h: "% from 52w Lo", f: r => fmtPct(r.pct_from_low)},
    "prepost":   {h: "Pre / Post",    f: r => `${fmtPct(r.premarket_pct)} / ${fmtPct(r.postmarket_pct)}`},
  }[kind] || {h: "", f: () => ""};

  el.innerHTML = `<table class="table table-sm mb-0">
    <thead><tr>
      <th>Sym</th><th>Name</th><th>Sector</th>
      <th class="text-end">Price</th><th class="text-end">Today %</th>
      <th class="text-end">Volume</th><th class="text-end">${extraCol.h}</th>
    </tr></thead><tbody>${rows.map(r => `
      <tr>
        <td><strong>${esc(r.symbol)}</strong></td>
        <td class="text-truncate" style="max-width:180px">${esc(r.name || "")}</td>
        <td><small style="color:${sectorColor(r.sector)}">${esc(r.sector || "")}</small></td>
        <td class="text-end">${fmtNum(r.price)}</td>
        <td class="text-end">${fmtPct(r.pct)}</td>
        <td class="text-end">${fmtCompactVol(r.volume)}</td>
        <td class="text-end">${extraCol.f(r)}</td>
      </tr>`).join("")}</tbody></table>`;
}

// ── Fear & Greed ──────────────────────────────────────────────────────────
function fgColor(score) {
  if (score == null) return "#9ca3af";
  if (score < 25) return "#d64545";
  if (score < 45) return "#f59e0b";
  if (score <= 55) return "#9ca3af";
  if (score <= 75) return "#84cc16";
  return "#1a9f53";
}
async function refreshFearGreed() {
  const d = await fetchJSON("/api/fear-greed");
  if (!d) return;
  const scoreEl = document.getElementById("fg-score");
  const labelEl = document.getElementById("fg-label");
  const score = d.score;
  scoreEl.textContent = score == null ? "—" : Math.round(score);
  scoreEl.style.color = fgColor(score);
  labelEl.innerHTML = d.label
    ? `<span style="color:${fgColor(score)}; font-weight:600">${esc(d.label)}</span>`
    : "";

  // Half-ring gauge using a doughnut with rotation=270°, circumference=180°
  const pct = score == null ? 0 : Math.max(0, Math.min(100, score));
  setChart("fg-gauge", {
    type: "doughnut",
    data: {
      datasets: [{
        data: [pct, 100 - pct],
        backgroundColor: [fgColor(score), "#e6edf7"],
        borderWidth: 0,
      }],
    },
    options: {
      rotation: 270, circumference: 180,
      cutout: "72%",
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      responsive: true, maintainAspectRatio: false,
    },
  });

  const el = document.getElementById("fg-components");
  el.innerHTML = (d.components || []).map(c => {
    const s = c.score;
    const pct = s == null ? 0 : Math.max(0, Math.min(100, s));
    const col = fgColor(s);
    return `<div class="fg-component-row">
      <span class="lbl">${esc(c.label)}</span>
      <span class="bar"><div style="width:${pct}%; background:${col}"></div></span>
      <span class="val" style="color:${col}">${s == null ? "—" : Math.round(s)}</span>
    </div>
    <div class="small text-muted" style="margin-left:150px; margin-bottom:.4rem">${esc(c.detail || "")}</div>`;
  }).join("");
}

// ── SPY Forward Volume Signal ─────────────────────────────────────────────
function signalClass(sig) {
  if (!sig) return "neu";
  if (sig === "Accumulation" || sig === "Bullish Confirm") return "pos";
  if (sig === "Distribution" || sig === "Bearish Confirm") return "neg";
  return "neu";
}
async function refreshSpyVolumeSignal() {
  const qs = buildRangeQS(state.spy_obv_view, "spy-obv-start", "spy-obv-end");
  const d = await fetchJSON(`/api/spy-volume-signal?${qs}`);
  if (!d) return;
  const pill = document.getElementById("spy-vol-signal");
  const cls = signalClass(d.signal);
  pill.innerHTML = d.signal
    ? `<span class="signal-pill ${cls}">${esc(d.signal)}</span>`
    : "";

  const meta = document.getElementById("spy-vol-meta");
  const vr = d.volume_ratio;
  const udr = d.up_down_ratio;
  const upV = d.up_vol || 0, dnV = d.down_vol || 0;
  const udPctUp = (upV + dnV) > 0 ? (upV / (upV + dnV) * 100) : null;
  meta.innerHTML = [
    d.reason ? `<em>${esc(d.reason)}</em>` : "",
    `<br>Today vol: <strong>${vr == null ? "—" : vr.toFixed(2) + "x"}</strong> of 20d avg`,
    ` · 20d up-vol/down-vol: <strong>${udr == null ? "—" : udr.toFixed(2)}</strong>`,
    udPctUp != null ? ` (${udPctUp.toFixed(0)}% up)` : "",
    ` · ${d.up_days || 0} up / ${d.down_days || 0} down days`,
  ].join("");

  const pts = d.points || [];
  if (!pts.length) return;
  setChart("spy-obv-chart", {
    type: "line",
    data: {
      labels: pts.map(p => labelForView(p.t, state.spy_obv_view)),
      datasets: [
        {
          label: "OBV (cumulative signed volume)",
          data: pts.map(p => p.value),
          borderColor: "#60a5fa",
          backgroundColor: "rgba(96,165,250,.15)",
          pointRadius: 0, tension: 0.25, borderWidth: 2,
          fill: true, yAxisID: "y",
        },
        {
          label: "SPY close",
          data: (d.price_points || []).map(p => p.value),
          borderColor: "#f59e0b",
          pointRadius: 0, tension: 0.25, borderWidth: 1.5,
          borderDash: [4, 3],
          yAxisID: "y1", fill: false,
        },
      ],
    },
    options: {
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { labels: { font: { size: 10 } } } },
      scales: {
        x: { ticks: { maxTicksLimit: 8, font: { size: 9 } } },
        y: { position: "left", ticks: { callback: v => fmtCompactVol(v) } },
        y1: { position: "right", grid: { drawOnChartArea: false },
              title: { display: true, text: "SPY $" } },
      },
    },
  });
}

// ── earnings ──────────────────────────────────────────────────────────────
function fmtMktCap(v) {
  if (v == null) return "—";
  if (v >= 1e12) return "$" + (v / 1e12).toFixed(2) + "T";
  if (v >= 1e9)  return "$" + (v / 1e9).toFixed(1) + "B";
  if (v >= 1e6)  return "$" + (v / 1e6).toFixed(0) + "M";
  return "$" + v;
}
function fmtIv(v) {
  if (v == null) return "—";
  return (v * 100).toFixed(0) + "%";
}
// render implied move and highlight vs historical average
function fmtImpliedMove(im, hist) {
  if (im == null) return "—";
  let cls = "";
  if (hist != null) {
    if (im > hist * 1.25) cls = "im-hot";
    else if (im < hist * 0.85) cls = "im-cool";
  }
  return `<span class="${cls}">±${(im * 100).toFixed(2)}%</span>`;
}
async function refreshEarnings() {
  const d = await fetchJSON("/api/earnings?days=7");
  const el = document.getElementById("earnings-body");
  if (!d || !d.length) {
    el.innerHTML = `<tr><td colspan="11" class="text-muted text-center py-3">No upcoming reports cached yet.</td></tr>`;
    return;
  }
  el.innerHTML = d.map(e => `
    <tr>
      <td>${esc(e.report_date)}<br><small class="text-muted">${esc(e.when_reported || "")}</small></td>
      <td><strong>${esc(e.symbol)}</strong></td>
      <td class="text-truncate" style="max-width:200px">${esc(e.name || "")}</td>
      <td class="text-end">${e.price != null ? fmtNum(e.price) : "—"}</td>
      <td class="text-end">${fmtPct(e.pct_change)}</td>
      <td class="text-end">${e.eps_estimate != null ? fmtNum(e.eps_estimate) : "—"}</td>
      <td class="text-end">${fmtPct(e.last_surprise_pct, 1)}</td>
      <td class="text-end">${fmtIv(e.iv_30d)}</td>
      <td class="text-end">${fmtImpliedMove(e.implied_move_pct, e.hist_avg_move_pct)}</td>
      <td class="text-end">${e.hist_avg_move_pct != null ? "±" + (e.hist_avg_move_pct * 100).toFixed(2) + "%" : "—"}</td>
      <td class="text-end">${fmtMktCap(e.market_cap)}</td>
    </tr>`).join("");
}

function fmtRatio(v) {
  if (v == null || Number.isNaN(v)) return "—";
  return (v * 100).toFixed(1) + "%";
}

function formatHoldingsText(holdings) {
  return (holdings || []).map(h =>
    `${h.symbol}: ${Number(h.quantity).toString()}`
  ).join("\n");
}

function parseHoldingsText(text) {
  const out = [];
  const lines = String(text || "").split(/\r?\n/);
  for (const ln of lines) {
    const s = ln.trim();
    if (!s) continue;
    const m = s.match(/^([A-Za-z0-9.\-^]+)\s*[:\s]\s*([0-9]*\.?[0-9]+)$/);
    if (!m) continue;
    out.push({ symbol: m[1].toUpperCase(), quantity: Number(m[2]) });
  }
  return out;
}

async function refreshDividendProfiles(selected = null) {
  const list = await fetchJSON("/api/dividend-profiles");
  const sel = document.getElementById("div-profile-select");
  if (!sel) return;
  const profiles = list || [];
  if (!profiles.length) {
    sel.innerHTML = `<option value="default">default</option>`;
    state.div_profile = "default";
    return;
  }
  const pick = selected || state.div_profile || profiles[0].name;
  sel.innerHTML = profiles.map(p =>
    `<option value="${esc(p.name)}">${esc(p.name)} (${p.holdings_count || 0})</option>`
  ).join("");
  const exists = profiles.some(p => p.name === pick);
  state.div_profile = exists ? pick : profiles[0].name;
  sel.value = state.div_profile;
}

async function loadDividendProfile(name) {
  const target = name || document.getElementById("div-profile-select")?.value || "default";
  const d = await fetchJSON(`/api/dividend-profile?name=${encodeURIComponent(target)}`);
  if (!d) return;
  state.div_profile = d.name || target;
  const nameInput = document.getElementById("div-profile-name");
  const holdingsInput = document.getElementById("div-holdings-input");
  if (nameInput) nameInput.value = state.div_profile;
  if (holdingsInput) holdingsInput.value = formatHoldingsText(d.holdings || []);
  const meta = document.getElementById("div-profile-meta");
  if (meta) meta.textContent = `${(d.holdings || []).length} holdings`;
}

async function loadSelectedDividendProfile() {
  await loadDividendProfile();
  await refreshDividendReport();
}

async function saveDividendProfile() {
  const name = (document.getElementById("div-profile-name")?.value || "").trim() || "default";
  const text = document.getElementById("div-holdings-input")?.value || "";
  const holdings = parseHoldingsText(text);
  const r = await fetch(withBasePath("/api/dividend-profile"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, holdings }),
  });
  const d = r.ok ? await r.json() : null;
  if (!d || !d.ok) return;
  state.div_profile = d.name || name;
  await refreshDividendProfiles(state.div_profile);
  await loadDividendProfile(state.div_profile);
  await refreshDividendReport();
}

async function duplicateDividendProfile() {
  const current = state.div_profile || "default";
  const source = await fetchJSON(`/api/dividend-profile?name=${encodeURIComponent(current)}`);
  if (!source) return;
  const baseName = `${current}-copy`;
  const ask = window.prompt("Duplicate profile name:", baseName);
  if (!ask) return;
  const name = ask.trim().toLowerCase();
  if (!name) return;
  const r = await fetch(withBasePath("/api/dividend-profile"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, holdings: source.holdings || [] }),
  });
  const d = r.ok ? await r.json() : null;
  if (!d || !d.ok) return;
  state.div_profile = d.name || name;
  await refreshDividendProfiles(state.div_profile);
  await loadDividendProfile(state.div_profile);
  await refreshDividendReport();
}

async function deleteDividendProfile() {
  const current = state.div_profile || "default";
  if (current === "default") {
    window.alert("default profile cannot be deleted");
    return;
  }
  if (!window.confirm(`Delete profile "${current}"?`)) return;
  const r = await fetch(withBasePath("/api/dividend-profile"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "delete", name: current }),
  });
  const d = r.ok ? await r.json() : null;
  if (!d || !d.ok) {
    if (d?.error) window.alert(d.error);
    return;
  }
  state.div_profile = "default";
  await refreshDividendProfiles(state.div_profile);
  await loadDividendProfile(state.div_profile);
  await refreshDividendReport();
}

async function refreshDividendReport() {
  const d = await fetchJSON(`/api/dividends?profile=${encodeURIComponent(state.div_profile || "default")}`);
  if (!d) return;
  document.getElementById("div-asof").textContent = d.as_of ? `as of ${d.as_of.replace("T", " ")}` : "";

  const rows = d.rows || [];
  const summary = document.getElementById("div-summary-body");
  if (!rows.length) {
    summary.innerHTML = `<tr><td colspan="4" class="text-muted text-center py-3">no data</td></tr>`;
  } else {
    summary.innerHTML = rows.map(r => `
      <tr>
        <td><strong>${esc(r.symbol)}</strong></td>
        <td class="text-end">${fmtNum(r.quantity, 4)}</td>
        <td class="text-end">${r.dividend_per_share != null ? "$" + fmtNum(r.dividend_per_share, 4) : "—"}</td>
        <td class="text-end">${r.est_payout != null ? "$" + fmtNum(r.est_payout, 2) : "—"}</td>
      </tr>
    `).join("");
  }

  setChart("dividend-chart", {
    type: "bar",
    data: {
      labels: rows.map(r => r.symbol),
      datasets: [{
        label: "Estimated cash (qty × last historical dividend/share)",
        data: rows.map(r => r.est_payout ?? 0),
        backgroundColor: "#34d399",
        borderColor: "#10b981",
        borderWidth: 1,
      }],
    },
    options: {
      plugins: { legend: { display: false } },
      scales: {
        y: { title: { display: true, text: "$ payout" } },
        x: { ticks: { maxRotation: 0, minRotation: 0 } },
      },
    },
  });

  const byDate = {};
  (d.events || []).forEach(e => {
    const ex = e.ex_date;
    if (!ex) return;
    if (!byDate[ex]) byDate[ex] = 0;
    byDate[ex] += Number(e.est_payout || 0);
  });
  const calDates = Object.keys(byDate).sort().slice(-24);
  setChart("dividend-calendar-chart", {
    type: "line",
    data: {
      labels: calDates,
      datasets: [{
        label: "Historical dividend calendar (est payout by ex-date)",
        data: calDates.map(dte => byDate[dte]),
        borderColor: "#2563eb",
        backgroundColor: "rgba(37,99,235,.15)",
        fill: true,
        tension: 0.25,
        pointRadius: 2,
        borderWidth: 2,
      }],
    },
    options: {
      plugins: { legend: { position: "top" } },
      scales: {
        y: { title: { display: true, text: "$ payout" } },
        x: { ticks: { maxTicksLimit: 10 } },
      },
    },
  });
}
