// -- shared utilities -------------------------------------------------------
function esc(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function statCard(label, value, cls) {
  return `<div class="stat-box text-center">
    <div class="text-muted small">${label}</div>
    <div class="stat-val ${cls}">${value}</div>
  </div>`;
}

function pnlClass(v) {
  return v > 0 ? "pos" : v < 0 ? "neg" : "neu";
}

function pnlFmt(v) {
  return (v >= 0 ? "+" : "") + "$" + Math.abs(v).toFixed(3);
}

// -- CRYPTO 5m ---------------------------------------------------------------
const C_COLORS = {
  "1c": { border: "#f87171", bg: "rgba(248,113,113,0.12)" },
  "2c": { border: "#fbbf24", bg: "rgba(251,191,36,0.12)" },
  "3c": { border: "#34d399", bg: "rgba(52,211,153,0.12)" },
};
let cryptoCharts = {};

async function loadCrypto() {
  const d = await fetch("/api/data").then(r => r.json());
  const el = document.getElementById("crypto-content");
  if (d.error) {
    el.innerHTML = `<div class="alert alert-warning">${esc(d.error)}</div>`;
    return;
  }
  document.getElementById("crypto-meta").textContent = `${d.total_candles} candles  ${d.total_resolved} resolved  last signal: ${d.last_update}`;

  el.innerHTML = "";
  d.assets.forEach(asset => {
    const ad = d.data[asset];
    let boxes = "";
    ["1c", "2c", "3c"].forEach(t => {
      const s = ad.stats[t];
      const pc = pnlClass(s.total_pnl);
      const lbl = t === "1c" ? " 1" : t === "2c" ? " 2" : " 3";
      boxes += `
        <div class="col">
          <div class="stat-box h-100">
            <div class="d-flex justify-content-between align-items-center mb-2">
              <span class="fw-bold" style="color:${C_COLORS[t].border}">${lbl}</span>
              <span class="badge bg-secondary">${s.resolved} resolved / ${s.pending} pending</span>
            </div>
            <div class="row g-2 text-center">
              <div class="col-4">
                <div class="text-muted small">Win rate</div>
                <div class="stat-val">${s.win_rate}%</div>
                <div class="text-muted small">${s.wins}W / ${s.losses}L</div>
              </div>
              <div class="col-4">
                <div class="text-muted small">Net P/L</div>
                <div class="stat-val ${pc}">${s.total_pnl >= 0 ? "+" : ""}$${s.total_pnl.toFixed(3)}</div>
              </div>
              <div class="col-4">
                <div class="text-muted small">ROI</div>
                <div class="stat-val ${pc}">${s.roi >= 0 ? "+" : ""}${s.roi.toFixed(1)}%</div>
                <div class="text-muted small">${s.signals} signals</div>
              </div>
            </div>
          </div>
        </div>`;
    });
    el.innerHTML += `
      <div class="card mb-4">
        <div class="card-header fw-bold fs-5">${esc(asset)}</div>
        <div class="card-body">
          <div class="row g-3 mb-3">${boxes}</div>
          <canvas id="chart-${asset}" style="max-height:320px"></canvas>
        </div>
      </div>`;
  });
  setTimeout(() => d.assets.forEach(a => buildCryptoChart(a, d.data[a])), 50);
}

function buildCryptoChart(asset, ad) {
  const ctx = document.getElementById(`chart-${asset}`);
  if (!ctx) return;
  if (cryptoCharts[asset]) cryptoCharts[asset].destroy();
  cryptoCharts[asset] = new Chart(ctx, {
    type: "line",
    data: {
      labels: ad.chart.labels,
      datasets: [
        {
          label: "1",
          data: ad.chart.series_1c,
          borderColor: C_COLORS["1c"].border,
          backgroundColor: C_COLORS["1c"].bg,
          fill: true,
          tension: 0.3,
          pointRadius: 2,
          borderWidth: 2,
        },
        {
          label: "2",
          data: ad.chart.series_2c,
          borderColor: C_COLORS["2c"].border,
          backgroundColor: C_COLORS["2c"].bg,
          fill: true,
          tension: 0.3,
          pointRadius: 2,
          borderWidth: 2,
        },
        {
          label: "3",
          data: ad.chart.series_3c,
          borderColor: C_COLORS["3c"].border,
          backgroundColor: C_COLORS["3c"].bg,
          fill: true,
          tension: 0.3,
          pointRadius: 2,
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "top" },
        tooltip: {
          callbacks: {
            label: c => ` ${c.dataset.label}: ${c.parsed.y >= 0 ? "+" : ""}$${c.parsed.y.toFixed(3)}`,
          },
        },
      },
      scales: {
        x: {
          ticks: { color: "#8b949e", maxTicksLimit: 16, maxRotation: 45 },
          grid: { color: "#21262d" },
        },
        y: {
          ticks: {
            color: "#8b949e",
            callback: v => (v >= 0 ? "+" : "") + "$" + v.toFixed(2),
          },
          grid: { color: "#21262d" },
          title: { display: true, text: "Cumulative P/L (USDC)", color: "#8b949e" },
        },
      },
    },
  });
}

// -- ETH 1H -------------------------------------------------------------------
let ethChart = null;
let allTrades = [];
let ethChartData = null;
let ethCurrentPage = 0;
const ETH_PAGE_SIZE = 20;
let ethTimeFilter = "all";

async function loadEth() {
  const d = await fetch("/api/eth").then(r => r.json());
  if (d.error) {
    document.getElementById("eth-stats").innerHTML = `<div class="alert alert-warning">${esc(d.error)}</div>`;
    return;
  }
  const s = d.stats;
  const pc = pnlClass(s.total_pnl);
  document.getElementById("eth-stats").innerHTML = [
    statCard("Total", s.total, ""),
    statCard("Wins", s.wins, "pos"),
    statCard("Losses", s.losses, "neg"),
    statCard("Pending", s.pending, "neu"),
    statCard("Win Rate", s.resolved ? s.win_rate + "%" : "--", s.win_rate >= 50 ? "pos" : "neg"),
    statCard("Net P/L", pnlFmt(s.total_pnl), pc),
    statCard("ROI", (s.roi >= 0 ? "+" : "") + s.roi.toFixed(1) + "%", pc),
    statCard("Total Cost", "$" + s.total_cost.toFixed(2), ""),
  ].join("");

  allTrades = d.trades;
  ethChartData = d.chart;
  ethCurrentPage = 0;
  renderTrades();
  buildEthChart(ethChartData);
}

function filterEthChart(period) {
  ethTimeFilter = period;
  ethCurrentPage = 0;

  // Update button styles
  document.querySelectorAll("#ethSubTabs button").forEach(btn => btn.style.display = "none");
  ["1d", "1w", "1y", "all"].forEach(p => {
    const btn = document.getElementById(`btn-${p}`);
    if (btn) {
      btn.classList.toggle("active", p === period);
      btn.style.display = "inline-block";
    }
  });

  buildEthChart(ethChartData);
}

function getEthFilteredData(period) {
  if (!ethChartData) return ethChartData;

  const now = Date.now();
  let cutoffMs = 0;

  if (period === "1d") cutoffMs = now - 24 * 60 * 60 * 1000;
  else if (period === "1w") cutoffMs = now - 7 * 24 * 60 * 60 * 1000;
  else if (period === "1y") cutoffMs = now - 365 * 24 * 60 * 60 * 1000;

  if (period === "all") return ethChartData;

  // Find start index based on cutoff
  const labels = ethChartData.labels || [];
  const series = ethChartData.series || [];

  let startIdx = 0;
  for (let i = 0; i < labels.length; i++) {
    const dateStr = labels[i]; // "MM/DD HH:MM"
    // Approximate: we'll keep recent data, rough filter
    startIdx = Math.max(0, series.length - Math.ceil((now - cutoffMs) / (30 * 60 * 1000)));
  }

  return {
    labels: labels.slice(startIdx),
    series: series.slice(startIdx),
  };
}

function renderTrades() {
  const showDry = document.getElementById("showDryRun").checked;
  const trades = allTrades.filter(t => showDry || !t.dry_run);

  const totalPages = Math.ceil(trades.length / ETH_PAGE_SIZE);
  const startIdx = ethCurrentPage * ETH_PAGE_SIZE;
  const endIdx = startIdx + ETH_PAGE_SIZE;
  const pageData = trades.slice(startIdx, endIdx);

  const tbody = document.getElementById("eth-trades-body");
  const link = document.getElementById("eth-market-link");

  if (!pageData.length) {
    tbody.innerHTML = `<tr><td colspan="10" class="text-center text-muted py-3">No trades yet.</td></tr>`;
    document.getElementById("eth-pagination-info").textContent = "";
    if (link) link.style.display = "none";
    return;
  }

  // Show link to most recent trade's Polymarket page
  const mostRecent = trades[0];
  if (link && mostRecent.slug) {
    link.href = `https://polymarket.com/event/${mostRecent.slug}`;
    link.style.display = "inline-block";
  }

  tbody.innerHTML = pageData
    .map(t => {
      const time = (t.placed_at || "--").substring(0, 16).replace("T", " ");
      const fullSlug = t.slug || "--";
      const slug = fullSlug.split("-").slice(-4).join("-");
      const dir =
        t.direction === "Up"
          ? `<span class="dir-up">Up</span>`
          : `<span class="dir-down">Down</span>`;
      const mode = t.dry_run
        ? `<span class="pill pill-dry">dry</span>`
        : `<span class="badge bg-success">live</span>`;
      const oc = t.outcome;
      const pill =
        oc === "win"
          ? `<span class="pill pill-win">win</span>`
          : oc === "loss"
            ? `<span class="pill pill-loss">loss</span>`
            : oc === "stop_loss"
              ? `<span class="pill pill-loss">stop</span>`
              : oc === "expired"
                ? `<span class="pill pill-loss">expired</span>`
                : `<span class="pill pill-open">open</span>`;
      const pnl =
        t.pnl !== null
          ? `<span class="${pnlClass(t.pnl)}">${t.pnl >= 0 ? "+" : ""}$${Math.abs(t.pnl).toFixed(3)}</span>`
          : `<span class="text-muted">--</span>`;
      const mins = t.mins_remaining !== null ? t.mins_remaining.toFixed(1) : "--";
      return `<tr>
        <td><a href="https://polymarket.com/event/${esc(fullSlug)}" target="_blank" class="text-decoration-none" style="font-size:13px;color:#58a6ff;font-weight:600;padding:4px 6px;border-radius:4px" onmouseover="this.style.backgroundColor='#1e3a5f'" onmouseout="this.style.backgroundColor='transparent'">${esc(slug)}</a></td>
        <td>${esc(time)}</td>
        <td>${dir}</td>
        <td>${t.entry_price.toFixed(3)}</td>
        <td>${t.shares}</td>
        <td>$${(t.cost_usdc || 0).toFixed(2)}</td>
        <td>${mins}</td>
        <td>${mode}</td>
        <td>${pill}</td>
        <td>${pnl}</td>
      </tr>`;
    })
    .join("");

  document.getElementById("eth-pagination-info").textContent = `Page ${ethCurrentPage + 1} of ${totalPages} (${trades.length} total)`;
  document.getElementById("eth-prev-btn").disabled = ethCurrentPage === 0;
  document.getElementById("eth-next-btn").disabled = ethCurrentPage >= totalPages - 1;
}

function ethPrevPage() {
  if (ethCurrentPage > 0) {
    ethCurrentPage--;
    renderTrades();
  }
}

function ethNextPage() {
  const showDry = document.getElementById("showDryRun").checked;
  const trades = allTrades.filter(t => showDry || !t.dry_run);
  const totalPages = Math.ceil(trades.length / ETH_PAGE_SIZE);
  if (ethCurrentPage < totalPages - 1) {
    ethCurrentPage++;
    renderTrades();
  }
}

document.getElementById("showDryRun").addEventListener("change", () => {
  ethCurrentPage = 0;
  renderTrades();
});

function buildEthChart(chart) {
  const ctx = document.getElementById("eth-chart");
  if (!ctx) return;
  if (ethChart) ethChart.destroy();

  const filteredData = getEthFilteredData(ethTimeFilter);
  if (!filteredData) return;

  ethChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: filteredData.labels,
      datasets: [
        {
          label: "Cumulative P/L",
          data: filteredData.series,
          borderColor: "#58a6ff",
          backgroundColor: "rgba(88,166,255,0.12)",
          fill: true,
          tension: 0.3,
          pointRadius: 3,
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "top" },
        tooltip: {
          callbacks: {
            label: c => `  P/L: ${c.parsed.y >= 0 ? "+" : ""}$${c.parsed.y.toFixed(3)}`,
          },
        },
      },
      scales: {
        x: {
          ticks: { color: "#8b949e", maxTicksLimit: 16, maxRotation: 45 },
          grid: { color: "#21262d" },
        },
        y: {
          ticks: {
            color: "#8b949e",
            callback: v => (v >= 0 ? "+" : "") + "$" + v.toFixed(2),
          },
          grid: { color: "#21262d" },
          title: { display: true, text: "Cumulative P/L (USDC)", color: "#8b949e" },
        },
      },
    },
  });
}

// -- log -----------------------------------------------------------------------
const NOISE_RE = /outside entry window|min remaining/;

async function loadLog() {
  const d = await fetch("/api/log").then(r => r.json());
  const hide = document.getElementById("hideNoise").checked;
  const logBox = document.getElementById("log-box");
  const lines = hide ? d.lines.filter(l => !NOISE_RE.test(l)) : d.lines;
  logBox.innerHTML = lines
    .map(raw => {
      const l = raw.trimEnd();
      let cls = "log-info";
      if (/ERROR|CRITICAL/i.test(l)) cls = "log-error";
      else if (/WARN/i.test(l)) cls = "log-warn";
      else if (/win|redeemed/i.test(l)) cls = "log-win";
      else if (/loss|stop_loss|expired/i.test(l)) cls = "log-loss";
      else if (/BUY|TRADE|DRY.RUN|placed|entry|signal/i.test(l)) cls = "log-trade";
      else if (NOISE_RE.test(l)) cls = "log-dim";
      return `<div class="${cls}">${esc(l)}</div>`;
    })
    .join("");
  document.getElementById("log-ts").textContent = "updated " + new Date().toLocaleTimeString();
  if (document.getElementById("autoScroll").checked) logBox.scrollTop = logBox.scrollHeight;
}
document.getElementById("hideNoise").addEventListener("change", loadLog);

// -- weather ------------------------------------------------------------------
async function loadWeather() {
  const d = await fetch("/api/weather").then(r => r.json());
  const pc = pnlClass(d.stats?.total_pnl ?? 0);
  if (d.error) {
    document.getElementById("weather-stats").innerHTML = `<div class="alert alert-warning">${esc(d.error)}</div>`;
    return;
  }
  const s = d.stats;
  document.getElementById("weather-stats").innerHTML = [
    statCard("Total Bets", s.total, ""),
    statCard("Wins", s.wins, "pos"),
    statCard("Losses", s.losses, "neg"),
    statCard("Pending", s.pending, "neu"),
    statCard("Win Rate", s.resolved ? s.win_rate + "%" : "--", s.win_rate >= 50 ? "pos" : "neg"),
    statCard("Net P/L", pnlFmt(s.total_pnl), pc),
    statCard("Total Cost", "$" + s.total_cost.toFixed(2), ""),
  ].join("");

  const tbody = document.getElementById("weather-body");
  if (!d.bets.length) {
    tbody.innerHTML = `<tr><td colspan="12" class="text-center text-muted py-3">No bets yet.</td></tr>`;
    return;
  }
  tbody.innerHTML = d.bets
    .map(b => {
      const time = (b.placed_at_utc || "--").substring(0, 16).replace("T", " ");
      const q =
        b.question && b.question.length > 48
          ? b.question.substring(0, 48) + ""
          : b.question || "--";
      const mrg =
        b.forecast_minus_threshold !== null
          ? `<span class="${b.forecast_minus_threshold <= 0 ? "pos" : "neg"}">${b.forecast_minus_threshold >= 0 ? "+" : ""}${b.forecast_minus_threshold.toFixed(1)}</span>`
          : "--";
      const oc =
        b.outcome === "win"
          ? `<span class="pill pill-win">win</span>`
          : b.outcome === "loss"
            ? `<span class="pill pill-loss">loss</span>`
            : `<span class="pill pill-open">open</span>`;
      return `<tr>
        <td>${esc(time)}</td>
        <td><b>${esc(b.city)}</b></td>
        <td>${esc(b.event_date || "--")}</td>
        <td title="${esc(b.question || "")}">${esc(q)}</td>
        <td>${b.shares}</td>
        <td>${b.no_price !== null ? b.no_price.toFixed(3) : "--"}</td>
        <td>$${(b.cost_usdc || 0).toFixed(2)}</td>
        <td>${b.forecast_high !== null ? b.forecast_high.toFixed(1) : "--"}</td>
        <td>${mrg}</td>
        <td>${oc}</td>
        <td>${b.resolved_temp !== null ? b.resolved_temp : "--"}</td>
        <td>
          <div style="display:inline-flex;gap:4px;align-items:center">
            <input type="number" step="0.1" placeholder="temp" class="edit-input"
              id="rt-${b.id}" value="${b.resolved_temp !== null ? b.resolved_temp : ""}">
            <select class="edit-select" id="oc-${b.id}">
              <option value=""   ${!b.outcome ? "selected" : ""}>--</option>
              <option value="win"  ${b.outcome === "win" ? "selected" : ""}>win</option>
              <option value="loss" ${b.outcome === "loss" ? "selected" : ""}>loss</option>
            </select>
            <button class="edit-btn" onclick="updateWeather(${b.id})"></button>
          </div>
        </td>
      </tr>`;
    })
    .join("");
}

async function updateWeather(id) {
  const rt = document.getElementById(`rt-${id}`).value;
  const oc = document.getElementById(`oc-${id}`).value;
  await fetch(`/api/weather/update/${id}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      resolved_temp: rt ? parseFloat(rt) : null,
      outcome: oc || null,
    }),
  });
  loadWeather();
}

// -- BS Forward Test ----------------------------------------------------------
let ftCharts = {};
let ftLoaded = false;
let ftChartData = {};
let ftTimeFilter = "all";

function filterFtChart(period) {
  ftTimeFilter = period;

  // Update button styles
  ["12h", "1d", "1w", "1m", "all"].forEach(p => {
    const btn = document.getElementById(`ft-btn-${p}`);
    if (btn) {
      btn.classList.toggle("active", p === period);
    }
  });

  // Rebuild charts for all assets
  Object.keys(ftChartData).forEach(asset => {
    buildFtChart(asset, ftChartData[asset]);
  });
}

function getFtFilteredData(asset, period) {
  const ad = ftChartData[asset];
  if (!ad || !ad.chart) return ad?.chart;

  const now = Date.now();
  let cutoffMs = 0;

  if (period === "12h") cutoffMs = now - 12 * 60 * 60 * 1000;
  else if (period === "1d") cutoffMs = now - 24 * 60 * 60 * 1000;
  else if (period === "1w") cutoffMs = now - 7 * 24 * 60 * 60 * 1000;
  else if (period === "1m") cutoffMs = now - 30 * 24 * 60 * 60 * 1000;

  if (period === "all") return ad.chart;

  // Filter based on timestamps in labels (approximate by count)
  const labels = ad.chart.labels || [];
  const series = ad.chart.series || [];

  if (labels.length === 0) return ad.chart;

  // Estimate: keep last N points based on time period
  const timePerPoint = (now - cutoffMs) / (24 * 60 * 60 * 1000); // rough estimate in days
  const estimatedPoints = Math.ceil(timePerPoint * (labels.length / 30)); // assume 30 days of data
  const startIdx = Math.max(0, labels.length - estimatedPoints);

  return {
    labels: labels.slice(startIdx),
    series: series.slice(startIdx),
  };
}

async function loadFT() {
  ftLoaded = true;
  const minEdge = parseFloat(document.getElementById("ft-min-edge").value) || 0;
  const d = await fetch(`/api/bs-forward?min_edge=${minEdge}`).then(r => r.json());

  const globalEl = document.getElementById("ft-global-stats");
  const contentEl = document.getElementById("ft-content");
  const metaEl = document.getElementById("ft-meta");

  if (d.error) {
    globalEl.innerHTML = `<div class="alert alert-warning">${esc(d.error)}</div>`;
    contentEl.innerHTML = "";
    return;
  }

  const pc = pnlClass(d.total_pnl);
  metaEl.textContent = `${d.total} BTC signals | real ask price, BS edge filter`;
  globalEl.innerHTML = [statCard("Total Signals", d.total, ""), statCard("Net P/L", pnlFmt(d.total_pnl), pc)].join("");

  contentEl.innerHTML = "";
  d.assets.forEach(asset => {
    const ad = d.data[asset];
    const s = ad.stats;
    const pc = pnlClass(s.total_pnl);

    const statBoxes = `
      <div class="col-auto"><div class="stat-box text-center">
        <div class="text-muted small">Signals</div>
        <div class="stat-val">${s.signals}</div>
        <div class="text-muted small">${s.resolved} resolved / ${s.pending} pending</div>
      </div></div>
      <div class="col-auto"><div class="stat-box text-center">
        <div class="text-muted small">Win Rate</div>
        <div class="stat-val ${s.win_rate >= 50 ? "pos" : "neg"}">${s.resolved ? s.win_rate + "%" : "--"}</div>
        <div class="text-muted small">${s.wins}W / ${s.losses}L</div>
      </div></div>
      <div class="col-auto"><div class="stat-box text-center">
        <div class="text-muted small">Net P/L</div>
        <div class="stat-val ${pc}">${s.total_pnl >= 0 ? "+" : ""}$${s.total_pnl.toFixed(4)}</div>
      </div></div>
      <div class="col-auto"><div class="stat-box text-center">
        <div class="text-muted small">ROI</div>
        <div class="stat-val ${pc}">${s.roi >= 0 ? "+" : ""}${s.roi.toFixed(1)}%</div>
        <div class="text-muted small">cost $${s.total_cost.toFixed(3)}</div>
      </div></div>
      <div class="col-auto"><div class="stat-box text-center">
        <div class="text-muted small">Avg BS Edge</div>
        <div class="stat-val ${s.avg_edge >= 0 ? "pos" : "neg"}">${s.avg_edge >= 0 ? "+" : ""}${(s.avg_edge * 100).toFixed(2)}%</div>
      </div></div>`;

    // Store trades for pagination
    const allTrades = ad.trades || [];
    const ftPageSize = 20;
    window[`ftData_${asset}`] = allTrades;
    window[`ftPage_${asset}`] = 0;

    const renderFtTrades = () => {
      const trades = window[`ftData_${asset}`] || [];
      const page = window[`ftPage_${asset}`] || 0;
      const startIdx = page * ftPageSize;
      const endIdx = startIdx + ftPageSize;
      const pageData = trades.slice(startIdx, endIdx);
      const maxPage = Math.ceil(trades.length / ftPageSize) || 1;

      const tbody = pageData
        .map(r => {
          const dt = new Date(r.signal_ts * 1000).toISOString().substring(0, 16).replace("T", " ");
          const dir =
            r.side === "Up"
              ? `<span class="dir-up">Up</span>`
              : `<span class="dir-down">Down</span>`;
          const won =
            r.won === 1
              ? `<span class="pill pill-win">win</span>`
              : r.won === 0
                ? `<span class="pill pill-loss">loss</span>`
                : `<span class="pill pill-open">open</span>`;
          const pnl =
            r.pnl != null
              ? `<span class="${pnlClass(r.pnl)}">${r.pnl >= 0 ? "+" : ""}$${Math.abs(r.pnl).toFixed(4)}</span>`
              : `<span class="text-muted">--</span>`;
          const edge = r.edge != null ? `<span class="${r.edge >= 0 ? "pos" : "neg"}">${(r.edge * 100).toFixed(2)}%</span>` : "--";
          const fair = r.fair_price != null ? r.fair_price.toFixed(4) : "--";
          const market = r.slug || "--";
          const shares = r.shares != null ? r.shares : "--";
          const cost =
            r.shares != null && r.entry_price != null ? "$" + (r.shares * r.entry_price).toFixed(4) : "--";
          return `<tr>
            <td><a href="https://polymarket.com/event/${esc(market)}" target="_blank" class="text-decoration-none" style="font-size:13px;color:#58a6ff;font-weight:600;padding:4px 6px;border-radius:4px" onmouseover="this.style.backgroundColor='#1e3a5f'" onmouseout="this.style.backgroundColor='transparent'">${esc(market)}</a></td>
            <td>${dir}</td>
            <td class="text-muted" style="font-size:11px">${esc(dt)}</td>
            <td>$${r.entry_price.toFixed(4)}</td>
            <td>${shares}</td>
            <td>${cost}</td>
            <td>${fair}</td>
            <td>${edge}</td>
            <td>${r.secs_remaining != null ? r.secs_remaining + "s" : "--"}</td>
            <td>${won}</td>
            <td>${pnl}</td>
          </tr>`;
        })
        .join("");

      document.getElementById(`ft-tbody-${asset}`).innerHTML = tbody || `<tr><td colspan="11" class="text-center text-muted py-3">No trades.</td></tr>`;
      document.getElementById(`ft-page-num-${asset}`).textContent = (page + 1);
      document.getElementById(`ft-page-max-${asset}`).textContent = maxPage;
    };

    contentEl.innerHTML += `
      <div class="card mb-4">
        <div class="card-header fw-bold fs-5">${esc(asset)}</div>
        <div class="card-body">
          <div class="row g-2 mb-3">${statBoxes}</div>
          <canvas id="ft-chart-${asset}" style="max-height:260px"></canvas>
          <div class="mt-3">
            <div style="max-height:600px;overflow-y:auto;border:1px solid #30363d;border-radius:6px">
              <table class="table table-sm mb-0">
                <thead style="position:sticky;top:0;z-index:1">
                  <tr>
                    <th>Market</th><th>Side</th><th>Time (UTC)</th>
                    <th>Ask (entry)</th><th>Shares</th><th>Cost</th>
                    <th>Fair price</th><th>BS Edge</th>
                    <th>Secs left</th><th>Result</th><th>P/L</th>
                  </tr>
                </thead>
                <tbody id="ft-tbody-${asset}"></tbody>
              </table>
            </div>
            <div class="mt-2 d-flex justify-content-between align-items-center small text-muted">
              <span>Page <span id="ft-page-num-${asset}">1</span> / <span id="ft-page-max-${asset}">1</span></span>
              <div class="d-flex gap-2">
                <button class="btn btn-sm btn-outline-secondary" onclick="ftPrevPage('${asset}')">← Prev</button>
                <button class="btn btn-sm btn-outline-secondary" onclick="ftNextPage('${asset}')">Next →</button>
              </div>
            </div>
          </div>
        </div>
      </div>`;

    // Store chart data for filtering
    ftChartData[asset] = ad;

    setTimeout(renderFtTrades, 10);
  });

  setTimeout(() => d.assets.forEach(a => buildFtChart(a, d.data[a])), 50);
  loadFTLog();
}

function buildFtChart(asset, ad) {
  const ctx = document.getElementById(`ft-chart-${asset}`);
  if (!ctx) return;
  if (ftCharts[asset]) ftCharts[asset].destroy();

  const filteredData = getFtFilteredData(asset, ftTimeFilter);
  if (!filteredData) return;

  ftCharts[asset] = new Chart(ctx, {
    type: "line",
    data: {
      labels: filteredData.labels,
      datasets: [
        {
          label: "Cumulative P/L",
          data: filteredData.series,
          borderColor: "#58a6ff",
          backgroundColor: "rgba(88,166,255,0.12)",
          fill: true,
          tension: 0.3,
          pointRadius: 3,
          borderWidth: 2,
        },
      ],
    },
    options: {
      responsive: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "top" },
        tooltip: {
          callbacks: {
            label: c => `  P/L: ${c.parsed.y >= 0 ? "+" : ""}$${c.parsed.y.toFixed(4)}`,
          },
        },
      },
      scales: {
        x: {
          ticks: { color: "#8b949e", maxTicksLimit: 16, maxRotation: 45 },
          grid: { color: "#21262d" },
        },
        y: {
          ticks: {
            color: "#8b949e",
            callback: v => (v >= 0 ? "+" : "") + "$" + v.toFixed(3),
          },
          grid: { color: "#21262d" },
          title: { display: true, text: "Cumulative P/L (USDC)", color: "#8b949e" },
        },
      },
    },
  });
}

function ftNextPage(asset) {
  const data = window[`ftData_${asset}`] || [];
  const pageSize = 20;
  const maxPage = Math.ceil(data.length / pageSize) || 1;
  const currentPage = window[`ftPage_${asset}`] || 0;
  const nextPage = Math.min(currentPage + 1, maxPage - 1);
  window[`ftPage_${asset}`] = nextPage;

  // Render the page
  const trades = window[`ftData_${asset}`] || [];
  const page = nextPage;
  const startIdx = page * pageSize;
  const endIdx = startIdx + pageSize;
  const pageData = trades.slice(startIdx, endIdx);

  const tbody = pageData
    .map(r => {
      const dt = new Date(r.signal_ts * 1000).toISOString().substring(0, 16).replace("T", " ");
      const dir = r.side === "Up" ? `<span class="dir-up">Up</span>` : `<span class="dir-down">Down</span>`;
      const won = r.won === 1 ? `<span class="pill pill-win">win</span>` : r.won === 0 ? `<span class="pill pill-loss">loss</span>` : `<span class="pill pill-open">open</span>`;
      const pnl = r.pnl != null ? `<span class="${pnlClass(r.pnl)}">${r.pnl >= 0 ? "+" : ""}$${Math.abs(r.pnl).toFixed(4)}</span>` : `<span class="text-muted">--</span>`;
      const edge = r.edge != null ? `<span class="${r.edge >= 0 ? "pos" : "neg"}">${(r.edge * 100).toFixed(2)}%</span>` : "--";
      const fair = r.fair_price != null ? r.fair_price.toFixed(4) : "--";
      const market = r.slug || "--";
      const shares = r.shares != null ? r.shares : "--";
      const cost = r.shares != null && r.entry_price != null ? "$" + (r.shares * r.entry_price).toFixed(4) : "--";
      return `<tr>
        <td><a href="https://polymarket.com/event/${esc(market)}" target="_blank" class="text-primary text-decoration-none" style="font-size:11px">${esc(market)}</a></td>
        <td>${dir}</td>
        <td class="text-muted" style="font-size:11px">${esc(dt)}</td>
        <td>$${r.entry_price.toFixed(4)}</td>
        <td>${shares}</td>
        <td>${cost}</td>
        <td>${fair}</td>
        <td>${edge}</td>
        <td>${r.secs_remaining != null ? r.secs_remaining + "s" : "--"}</td>
        <td>${won}</td>
        <td>${pnl}</td>
      </tr>`;
    })
    .join("");

  document.getElementById(`ft-tbody-${asset}`).innerHTML = tbody || `<tr><td colspan="11" class="text-center text-muted py-3">No trades.</td></tr>`;
  document.getElementById(`ft-page-num-${asset}`).textContent = (page + 1);
  document.getElementById(`ft-page-max-${asset}`).textContent = maxPage;
}

function ftPrevPage(asset) {
  const currentPage = window[`ftPage_${asset}`] || 0;
  const prevPage = Math.max(currentPage - 1, 0);
  window[`ftPage_${asset}`] = prevPage;

  // Render the page
  const trades = window[`ftData_${asset}`] || [];
  const pageSize = 20;
  const page = prevPage;
  const startIdx = page * pageSize;
  const endIdx = startIdx + pageSize;
  const pageData = trades.slice(startIdx, endIdx);
  const maxPage = Math.ceil(trades.length / pageSize) || 1;

  const tbody = pageData
    .map(r => {
      const dt = new Date(r.signal_ts * 1000).toISOString().substring(0, 16).replace("T", " ");
      const dir = r.side === "Up" ? `<span class="dir-up">Up</span>` : `<span class="dir-down">Down</span>`;
      const won = r.won === 1 ? `<span class="pill pill-win">win</span>` : r.won === 0 ? `<span class="pill pill-loss">loss</span>` : `<span class="pill pill-open">open</span>`;
      const pnl = r.pnl != null ? `<span class="${pnlClass(r.pnl)}">${r.pnl >= 0 ? "+" : ""}$${Math.abs(r.pnl).toFixed(4)}</span>` : `<span class="text-muted">--</span>`;
      const edge = r.edge != null ? `<span class="${r.edge >= 0 ? "pos" : "neg"}">${(r.edge * 100).toFixed(2)}%</span>` : "--";
      const fair = r.fair_price != null ? r.fair_price.toFixed(4) : "--";
      const market = r.slug || "--";
      const shares = r.shares != null ? r.shares : "--";
      const cost = r.shares != null && r.entry_price != null ? "$" + (r.shares * r.entry_price).toFixed(4) : "--";
      return `<tr>
        <td><a href="https://polymarket.com/event/${esc(market)}" target="_blank" class="text-primary text-decoration-none" style="font-size:11px">${esc(market)}</a></td>
        <td>${dir}</td>
        <td class="text-muted" style="font-size:11px">${esc(dt)}</td>
        <td>$${r.entry_price.toFixed(4)}</td>
        <td>${shares}</td>
        <td>${cost}</td>
        <td>${fair}</td>
        <td>${edge}</td>
        <td>${r.secs_remaining != null ? r.secs_remaining + "s" : "--"}</td>
        <td>${won}</td>
        <td>${pnl}</td>
      </tr>`;
    })
    .join("");

  document.getElementById(`ft-tbody-${asset}`).innerHTML = tbody || `<tr><td colspan="11" class="text-center text-muted py-3">No trades.</td></tr>`;
  document.getElementById(`ft-page-num-${asset}`).textContent = (page + 1);
  document.getElementById(`ft-page-max-${asset}`).textContent = maxPage;
}

async function loadFTLog() {
  const box = document.getElementById("ft-log-box");
  const ts = document.getElementById("ft-log-ts");
  if (!box || !ts) return;
  const d = await fetch("/api/bs-log").then(r => r.json());
  const lines = d.lines || [];
  box.innerHTML = lines
    .map(raw => {
      const l = raw.trimEnd();
      let cls = "log-info";
      if (/BET-OPEN/i.test(l)) cls = "log-trade";
      else if (/BET-RESOLVE|MARKET-RESOLVED/i.test(l)) cls = "log-win";
      else if (/ERROR|CRITICAL/i.test(l)) cls = "log-error";
      else if (/WARN/i.test(l)) cls = "log-warn";
      return `<div class="${cls}">${esc(l)}</div>`;
    })
    .join("");
  ts.textContent = "updated " + new Date().toLocaleTimeString();
  box.scrollTop = box.scrollHeight;
}

// -- bootstrap ----------------------------------------------------------------
function refreshAll() {
  loadEth();
  loadLog();
  loadWeather();
  if (ftLoaded) {
    loadFT();
    loadFTLog();
  }
}

loadEth();
setInterval(loadEth, 30_000);
loadLog();
setInterval(loadLog, 10_000);
loadWeather();
loadFTLog();
setInterval(loadFTLog, 10_000);
