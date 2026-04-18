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
const state = { svix_view: "5d", tsl_view: "5d" };

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
  refreshAll();
  setInterval(refreshQuotes, 30_000);
  setInterval(refreshSlow,   5 * 60_000);
});

async function refreshAll() {
  await Promise.all([
    refreshHealth(), refreshEcon(), refreshMacro(), refreshBreadth(),
    refreshVixTerm(), refreshVvix(),
    renderSvix(), renderTsl(),
    renderTrackingError("TSLL", "TSLA", 2,  "tsll-te-chart", "tsll-te"),
    renderTrackingError("TSLZ", "TSLA", -1, "tslz-te-chart", "tslz-te"),
    refreshSectors(), refreshCorr(), refreshVolScatter(), refreshBubble(),
    refreshMovers(), refreshScreeners(), refreshEarnings(),
  ]);
  document.getElementById("last-refresh").textContent =
    "last refresh " + new Date().toLocaleTimeString();
}

async function refreshQuotes() {
  refreshMacro(); refreshBreadth(); refreshSectors();
  refreshBubble(); refreshMovers(); refreshScreeners();
  refreshVixTerm();
  document.getElementById("last-refresh").textContent =
    "last refresh " + new Date().toLocaleTimeString();
}

async function refreshSlow() {
  refreshCorr(); refreshVolScatter();
  renderTrackingError("TSLL", "TSLA", 2,  "tsll-te-chart", "tsll-te");
  renderTrackingError("TSLZ", "TSLA", -1, "tslz-te-chart", "tslz-te");
  refreshEarnings(); refreshEcon();
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
async function renderTrackingError(pair, base, leverage, canvasId, labelId) {
  const d = await fetchJSON(`/api/tracking-error?pair=${pair}&base=${base}&leverage=${leverage}`);
  if (!d || !d.actual) return;
  setChart(canvasId, {
    type: "line",
    data: {
      labels: d.actual.map(p => p.t),
      datasets: [
        { label: pair, data: d.actual.map(p => p.value),
          borderColor: "#60a5fa", backgroundColor: "#60a5fa22",
          tension: 0.25, borderWidth: 2, pointRadius: 2 },
        { label: `synthetic ${leverage}x ${base}`,
          data: d.synthetic.map(p => p.value),
          borderColor: "#fbbf24", borderDash: [6, 4],
          tension: 0.25, borderWidth: 2, pointRadius: 2 },
      ],
    },
    options: {
      plugins: { legend: { position: "top" } },
      scales: { y: { title: { display: true, text: "rebased to 100" } } },
    },
  });
  if (d.tracking_error_pct != null) {
    const cls = d.tracking_error_pct < 0 ? "neg" : "pos";
    document.getElementById(labelId).innerHTML =
      `drift: <span class="${cls}">${d.tracking_error_pct.toFixed(2)}pts</span>`;
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

// ── earnings ──────────────────────────────────────────────────────────────
async function refreshEarnings() {
  const d = await fetchJSON("/api/earnings?days=7");
  const el = document.getElementById("earnings-body");
  if (!d || !d.length) {
    el.innerHTML = `<tr><td colspan="5" class="text-muted text-center py-3">No upcoming reports cached yet.</td></tr>`;
    return;
  }
  el.innerHTML = d.map(e => `
    <tr>
      <td>${esc(e.report_date)}</td>
      <td><strong>${esc(e.symbol)}</strong></td>
      <td class="text-truncate" style="max-width:240px">${esc(e.name || "")}</td>
      <td>${esc(e.when_reported || "—")}</td>
      <td class="text-end">${e.eps_estimate != null ? fmtNum(e.eps_estimate) : "—"}</td>
    </tr>`).join("");
}
