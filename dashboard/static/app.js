"use strict";

const $ = (s, r = document) => r.querySelector(s);
const charts = {};

const usd = (n) =>
  n === null || n === undefined ? "—" : "$" + Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const pctCell = (n) => {
  const c = n > 0 ? "pos" : n < 0 ? "neg" : "";
  const s = n > 0 ? "+" : "";
  return `<span class="${c}">${s}${Number(n).toFixed(2)}%</span>`;
};
const POLY = "https://polymarket.com";
/* Market title -> link to its Polymarket event page (falls back to plain text). */
function mlink(p) {
  const slug = p.event_slug || p.slug || "";
  const text = p.title || p.slug || "—";
  return slug ? `<a href="${POLY}/event/${slug}" target="_blank" rel="noopener">${text}</a>` : text;
}
const fmtTs = (ts, days) => {
  const d = new Date(ts * 1000);
  return days <= 7
    ? d.toLocaleString([], { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit" })
    : d.toLocaleDateString();
};
async function getJSON(url, opts = {}) {
  const init = { method: opts.method || "GET" };
  if (opts.body !== undefined) {
    init.headers = { "Content-Type": "application/json" };
    init.body = JSON.stringify(opts.body);
  }
  const r = await fetch(url, init);
  const j = await r.json();
  if (!r.ok || j.error) throw new Error(j.error || r.status);
  return j;
}
function lineChart(canvasId, labels, datasets) {
  const ctx = $("#" + canvasId);
  if (charts[canvasId]) charts[canvasId].destroy();
  charts[canvasId] = new Chart(ctx, {
    type: "line",
    data: { labels, datasets: datasets.map((d) => ({ ...d, tension: 0.15, pointRadius: 0, borderWidth: 2, spanGaps: true })) },
    options: {
      responsive: true,
      plugins: { legend: { display: datasets.length > 1, labels: { color: "#8b949e" } } },
      scales: {
        x: { ticks: { color: "#8b949e", maxTicksLimit: 8 }, grid: { color: "#2a3140" } },
        y: { ticks: { color: "#8b949e" }, grid: { color: "#2a3140" } },
      },
    },
  });
}
function table(headers, rows) {
  if (!rows.length) return '<div class="muted">No data.</div>';
  const th = headers.map((h) => `<th>${h}</th>`).join("");
  const tr = rows.map((r) => `<tr>${r.map((c) => `<td>${c}</td>`).join("")}</tr>`).join("");
  return `<table><thead><tr>${th}</tr></thead><tbody>${tr}</tbody></table>`;
}

/* ---- Sidebar ---- */
$("#sidebar-toggle").addEventListener("click", () =>
  document.body.classList.toggle("sidebar-collapsed")
);
$("#balance-range").addEventListener("change", () =>
  loadBalanceChart(Number($("#balance-range").value))
);
$("#pro-balance-range").addEventListener("change", () =>
  loadProBalance(Number($("#pro-balance-range").value))
);

/* ---- Nav ---- */
const loaded = {};
document.querySelectorAll(".nav-item").forEach((btn) => {
  btn.addEventListener("click", () => {
    const tab = btn.dataset.tab;
    document.querySelectorAll(".nav-item").forEach((b) => b.classList.toggle("active", b === btn));
    document.querySelectorAll(".tab").forEach((t) => t.classList.toggle("active", t.id === "tab-" + tab));
    location.hash = tab;
    activate(tab);
  });
});
function activate(tab) {
  if (loaded[tab]) return;
  loaded[tab] = true;
  ({ positions: loadPositions, pros: loadPros, markets: loadMarkets, backtester: initBacktester }[tab] || (() => {}))();
}

/* ---- Positions ---- */
async function loadPositions() {
  try {
    const me = await getJSON("/api/me");
    $("#m-total").textContent = usd(me.total);
    $("#m-portfolio").textContent = usd(me.portfolio_value);
    $("#m-cash").textContent = me.cash === null ? "n/a" : usd(me.cash);
    $("#m-user").textContent = me.username;
    $("#nav-addr").textContent = me.address || "no address set";
  } catch (e) {
    $("#m-total").textContent = "error";
  }
  try {
    const { positions } = await getJSON("/api/positions");
    $("#positions-table").innerHTML = table(
      ["Market", "Outcome", "Size", "Avg", "Now", "Value", "P&L"],
      positions.map((p) => [
        mlink(p), p.outcome, p.size, p.avg_price, p.cur_price, usd(p.value),
        pctCell(p.pnl_pct),
      ])
    );
  } catch (e) {
    $("#positions-table").innerHTML = `<div class="muted">error: ${e.message}</div>`;
  }
  loadBalanceChart(Number($("#balance-range").value));
}
async function loadBalanceChart(days) {
  try {
    const { history } = await getJSON("/api/balance-history?days=" + days);
    if (!history.length) {
      $("#balance-empty").hidden = false;
      if (charts["chart-balance"]) charts["chart-balance"].destroy();
      return;
    }
    $("#balance-empty").hidden = true;
    const labels = history.map((h) => fmtTs(h.ts, days));
    lineChart("chart-balance", labels, [
      { label: "P&L", data: history.map((h) => h.value), borderColor: "#2d6cdf" },
    ]);
  } catch (e) {}
}

/* ---- Pros ---- */
let prosWired = false;
function selectChip(chip) {
  $("#pros-list").querySelectorAll(".pro-chip").forEach((r) => r.classList.toggle("sel", r === chip));
  loadProDetail(chip.dataset.addr, chip.dataset.label);
}
async function loadPros() {
  if (!prosWired) {
    prosWired = true;
    $("#pro-add").addEventListener("submit", async (e) => {
      e.preventDefault();
      const label = $("#pro-label").value.trim();
      const address = $("#pro-addr").value.trim();
      if (!address) return;
      try {
        await getJSON("/api/pros", { method: "POST", body: { label, address } });
        $("#pro-label").value = "";
        $("#pro-addr").value = "";
        await loadPros();
      } catch (err) {
        alert("add failed: " + err.message);
      }
    });
    $("#pros-list").addEventListener("click", async (e) => {
      const x = e.target.closest(".pro-x");
      if (x) {
        e.stopPropagation();
        if (x.dataset.addr === curPro) curPro = null;
        await getJSON("/api/pros/" + encodeURIComponent(x.dataset.addr), { method: "DELETE" });
        await loadPros();
        return;
      }
      const chip = e.target.closest(".pro-chip");
      if (chip) selectChip(chip);
    });
  }
  try {
    const { pros } = await getJSON("/api/pros");
    if (!pros.length) {
      $("#pros-list").innerHTML = '<div class="muted">No pros yet — add a wallet above.</div>';
      return;
    }
    $("#pros-list").innerHTML = pros
      .map((p) => `<div class="pro-chip" data-addr="${p.address}" data-label="${p.label}"><span>${p.label}</span><span class="muted pro-val">…</span>${
        p.custom ? `<button class="x-btn pro-x" data-addr="${p.address}" title="Remove">✕</button>` : ""
      }</div>`)
      .join("");
    loadProValues();
    // Auto-open: keep current selection if still present, else open the first.
    const chips = [...$("#pros-list").querySelectorAll(".pro-chip")];
    const keep = chips.find((c) => c.dataset.addr === curPro);
    if (keep) keep.classList.add("sel");
    else if (chips[0]) selectChip(chips[0]);
  } catch (e) {
    $("#pros-list").innerHTML = `<div class="muted">error: ${e.message}</div>`;
  }
}
async function loadProValues() {
  try {
    const { values } = await getJSON("/api/pros/values");
    const list = $("#pros-list");
    const chips = [...list.querySelectorAll(".pro-chip")];
    chips.forEach((ch) => {
      const v = values[ch.dataset.addr];
      ch.querySelector(".pro-val").textContent = v == null ? "—" : usd(v);
    });
    // Re-sort biggest portfolio first now that values are in.
    chips
      .sort((a, b) => (values[b.dataset.addr] || 0) - (values[a.dataset.addr] || 0))
      .forEach((ch) => list.appendChild(ch));
  } catch (e) {}
}
let curPro = null;
async function loadProDetail(addr, label) {
  curPro = addr;
  $("#pro-title").innerHTML =
    `<a href="${POLY}/profile/${addr}" target="_blank" rel="noopener">${label} ↗</a>`;
  loadProBalance(Number($("#pro-balance-range").value));
  const [{ positions }, { activity }] = await Promise.all([
    getJSON(`/api/pros/${addr}/positions`),
    getJSON(`/api/pros/${addr}/activity?limit=30`),
  ]);
  $("#pro-positions").innerHTML = table(
    ["Market", "Outcome", "Size", "Value", "P&L"],
    positions.map((p) => [mlink(p), p.outcome, p.size, usd(p.value), pctCell(p.pnl_pct)])
  );
  $("#pro-activity").innerHTML = table(
    ["When", "Type", "Market", "Price", "USDC"],
    activity.map((a) => [new Date(a.ts * 1000).toLocaleString(), a.type, a.title, a.price, usd(a.usdc_size)])
  );
}
async function loadProBalance(days) {
  if (!curPro) return;
  try {
    const { history } = await getJSON(`/api/pros/${curPro}/value-history?days=${days}`);
    if (!history.length) {
      $("#pro-balance-empty").hidden = false;
      $("#chart-pro-balance").hidden = true;
      if (charts["chart-pro-balance"]) charts["chart-pro-balance"].destroy();
      return;
    }
    $("#pro-balance-empty").hidden = true;
    $("#chart-pro-balance").hidden = false;
    const labels = history.map((h) => fmtTs(h.ts, days));
    lineChart("chart-pro-balance", labels, [
      { label: "P&L", data: history.map((h) => h.value), borderColor: "#2ea043" },
    ]);
  } catch (e) {}
}

/* ---- Markets ---- */
let mktWired = false;
async function loadMarkets() {
  if (!mktWired) {
    mktWired = true;
    $("#mkt-symbol").addEventListener("change", loadMarketChart);
    $("#mkt-range").addEventListener("change", loadMarketChart);
    $("#quote-add").addEventListener("submit", async (e) => {
      e.preventDefault();
      const sym = $("#quote-input").value.trim();
      if (!sym) return;
      try {
        await getJSON("/api/stocks/watchlist", { method: "POST", body: { symbol: sym } });
        $("#quote-input").value = "";
        await loadMarkets();
      } catch (err) {
        alert("add failed: " + err.message);
      }
    });
    $("#quotes-table").addEventListener("click", async (e) => {
      const x = e.target.closest(".x-btn");
      if (x) {
        await getJSON("/api/stocks/watchlist/" + encodeURIComponent(x.dataset.sym), { method: "DELETE" });
        await loadMarkets();
        return;
      }
      const row = e.target.closest("tr[data-sym]");
      if (row) {
        $("#mkt-symbol").value = row.dataset.sym;
        loadMarketChart();
      }
    });
  }
  try {
    const wl = await getJSON("/api/stocks/watchlist");
    const all = [...wl.stocks, ...wl.crypto];
    const custom = new Set(wl.custom || []);
    const cur = $("#mkt-symbol").value;
    $("#mkt-symbol").innerHTML = all.map((s) => `<option>${s}</option>`).join("");
    if (all.includes(cur)) $("#mkt-symbol").value = cur;
    const { quotes } = await getJSON("/api/stocks/quotes");
    const th = ["Symbol", "Price", "Chg", "Chg %", ""].map((h) => `<th>${h}</th>`).join("");
    const tr = quotes
      .map((q) => {
        const rm = custom.has(q.symbol) ? `<button class="x-btn" data-sym="${q.symbol}" title="Remove">✕</button>` : "";
        return `<tr data-sym="${q.symbol}"><td>${q.symbol}</td><td>${q.price ?? "—"}</td><td>${q.change ?? "—"}</td><td>${
          q.change_pct != null ? pctCell(q.change_pct) : "—"
        }</td><td>${rm}</td></tr>`;
      })
      .join("");
    $("#quotes-table").innerHTML = quotes.length
      ? `<table><thead><tr>${th}</tr></thead><tbody>${tr}</tbody></table>`
      : '<div class="muted">No data.</div>';
    loadMarketChart();
  } catch (e) {
    $("#quotes-table").innerHTML = `<div class="muted">error: ${e.message}</div>`;
  }
}
function highlightMktRow() {
  const sym = $("#mkt-symbol").value;
  $("#quotes-table").querySelectorAll("tr[data-sym]").forEach((r) => r.classList.toggle("sel", r.dataset.sym === sym));
}
async function loadMarketChart() {
  highlightMktRow();
  const symbol = $("#mkt-symbol").value;
  const range = $("#mkt-range").value;
  const data = await getJSON(`/api/stocks/history?symbol=${symbol}&range=${range}`);
  lineChart("chart-market", data.points.map((p) => p.t), [
    { label: symbol, data: data.points.map((p) => p.close), borderColor: "#2d6cdf" },
  ]);
}

/* ---- Backtester (multi-run overlay) ---- */
const BT_COLORS = ["#2ea043", "#2d6cdf", "#db6d28", "#a371f7", "#f85149", "#e3b341", "#39c5cf", "#ec6cb9"];
const btRuns = [];

function btLabel(d) {
  return `${d.symbol} ${d.freq[0].toUpperCase()} $${d.amount}${Number(d.initial) ? "+$" + d.initial : ""}`;
}

function initBacktester() {
  $("#bt-form").addEventListener("submit", (e) => {
    e.preventDefault();
    addRun();
  });
  $("#bt-runs").addEventListener("click", (e) => {
    const x = e.target.closest(".x-btn");
    if (x) removeRun(Number(x.dataset.id));
  });
  loadSavedRuns();
}

/* Compute one run's equity curve from a saved/new definition. */
async function computeRun(def) {
  const q = new URLSearchParams({
    symbol: def.symbol, strategy: "dca", amount: def.amount,
    initial: def.initial, freq: def.freq, start: def.start, end: def.end,
  });
  const r = await getJSON("/api/backtest?" + q.toString());
  const s = r.dca;
  if (!s || !s.series.length) throw new Error(r.error || "no data");
  return { id: def.id, color: def.color, label: btLabel(def),
    points: s.series, metrics: s.metrics, ...def };
}

async function loadSavedRuns() {
  try {
    const { runs } = await getJSON("/api/backtest/runs");
    for (const def of runs) {
      try {
        btRuns.push(await computeRun(def));
      } catch (e) {}
    }
    renderBt();
  } catch (e) {}
}

async function addRun() {
  const def = {
    symbol: ($("#bt-symbol").value.trim() || "SPY").toUpperCase(),
    amount: Number($("#bt-amount").value || "0"),
    initial: Number($("#bt-initial").value || "0"),
    freq: $("#bt-freq").value,
    start: $("#bt-start").value,
    end: $("#bt-end").value,
    color: BT_COLORS[btRuns.length % BT_COLORS.length],
  };
  const btn = $("#bt-form button");
  btn.disabled = true;
  btn.textContent = "Adding…";
  try {
    const { id } = await getJSON("/api/backtest/runs", { method: "POST", body: def });
    btRuns.push(await computeRun({ ...def, id }));
    renderBt();
  } catch (e) {
    alert("backtest failed: " + e.message);
  } finally {
    btn.disabled = false;
    btn.textContent = "Add";
  }
}

async function removeRun(id) {
  try {
    await getJSON("/api/backtest/runs/" + id, { method: "DELETE" });
  } catch (e) {}
  const i = btRuns.findIndex((r) => r.id === id);
  if (i >= 0) btRuns.splice(i, 1);
  renderBt();
}

function renderBt() {
  // Union of all dates -> aligned datasets (gaps spanned).
  const labelSet = new Set();
  btRuns.forEach((run) => run.points.forEach((p) => labelSet.add(p.t)));
  const labels = [...labelSet].sort();
  const datasets = btRuns.map((run) => {
    const m = new Map(run.points.map((p) => [p.t, p.value]));
    return { label: run.label, data: labels.map((t) => (m.has(t) ? m.get(t) : null)), borderColor: run.color };
  });
  lineChart("chart-bt", labels, datasets.length ? datasets : [{ label: "—", data: [], borderColor: "#8b949e" }]);

  $("#bt-runs").innerHTML = btRuns
    .map((run) => {
      const x = run.metrics || {};
      return `<div class="bt-run-card">
        <button class="x-btn" data-id="${run.id}" title="Remove">✕</button>
        <div class="bt-run-title"><span class="swatch" style="background:${run.color}"></span>${run.label}</div>
        <div class="muted small">${run.start} → ${run.end}</div>
        ${table(["Metric", "Value"], [
          ["Invested", usd(x.invested)],
          ["Final value", usd(x.final_value)],
          ["Total return", pctCell(x.total_return_pct)],
          ["CAGR", pctCell(x.cagr)],
          ["Max drawdown", pctCell(x.max_drawdown)],
        ])}
      </div>`;
    })
    .join("");
}

/* ---- Boot ---- */
const initial = (location.hash || "#positions").slice(1);
const btn = document.querySelector(`.nav-item[data-tab="${initial}"]`);
if (btn) btn.click();
else activate("positions");
