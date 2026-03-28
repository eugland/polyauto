const EVENT_HORIZON_HOURS = 30;
const EVENTS_PAGE_SIZE = 10;
const EVENTS_MAX_PAGES = 20;
const POLYMARKET_EVENTS_URL = "https://gamma-api.polymarket.com/events";
const WEATHER_CURRENT_URL = "https://api.weather.com/v3/wx/observations/current";
const METAR_HISTORY_URL = "https://aviationweather.gov/api/data/metar";
const WORKER_URL = "https://gentle-flower-99e9.eugene-r-w-12.workers.dev";

const EVENT_SLUG_RE = /^highest-temperature-in-(.+)-on-[a-z]+-\d{1,2}-\d{4}$/;
const EVENT_DATE_IN_TITLE_RE = /\bon\s+([A-Za-z]+)\s+(\d{1,2})(?:,)?\s+(\d{4})\b/i;
const EVENT_DATE_IN_SLUG_RE = /-on-([a-z]+)-(\d{1,2})-(\d{4})$/;

function addHours(date, hours) {
  return new Date(date.getTime() + hours * 60 * 60 * 1000);
}

function getEndDateMaxIso() {
  return addHours(new Date(), EVENT_HORIZON_HOURS).toISOString();
}

function toSearchParams(params) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    search.append(key, String(value));
  });
  return search.toString();
}

async function fetchExternalJson(url, errorPrefix) {
  const proxyUrl = `${WORKER_URL}?url=${encodeURIComponent(url)}`;
  const response = await fetch(proxyUrl);
  if (!response.ok) {
    throw new Error(`${errorPrefix}: HTTP ${response.status}`);
  }
  return response.json();
}

async function fetchTemperatureMarketsPayload() {
  const endDateMaxIso = getEndDateMaxIso();
  const events = [];
  const seenEventIds = new Set();

  for (let pageIndex = 0; pageIndex < EVENTS_MAX_PAGES; pageIndex += 1) {
    const offset = pageIndex * EVENTS_PAGE_SIZE;
    const query = toSearchParams({
      tag_slug: "temperature",
      closed: "false",
      limit: EVENTS_PAGE_SIZE,
      offset,
      end_date_max: endDateMaxIso,
    });
    const pageEvents = await fetchExternalJson(
      `${POLYMARKET_EVENTS_URL}?${query}`,
      "Polymarket API failed"
    );

    if (!Array.isArray(pageEvents) || pageEvents.length === 0) {
      break;
    }

    pageEvents.forEach((event) => {
      const key = String(event && event.id ? event.id : "");
      if (key && seenEventIds.has(key)) {
        return;
      }
      if (key) {
        seenEventIds.add(key);
      }
      events.push(event);
    });

    if (pageEvents.length < EVENTS_PAGE_SIZE) {
      break;
    }
  }

  const markets = [];

  events.forEach((event) => {
    const eventId = event.id;
    const eventTitle = event.title;
    const eventSlug = event.slug;
    (event.markets || []).forEach((market) => {
      markets.push({
        ...market,
        event_id: eventId,
        event_title: eventTitle,
        event_slug: eventSlug,
      });
    });
  });

  return { market_count: markets.length, markets };
}

function normalizeSource(item) {
  const src = item && typeof item.source === "object" ? item.source : {};
  const out = {};
  Object.entries(src).forEach(([key, value]) => {
    const sourceName = String(key || "").trim().toLowerCase();
    const sourceUrl = String(value || "").trim();
    if (sourceName && sourceUrl) {
      out[sourceName] = sourceUrl;
    }
  });
  return out;
}

function loadLocationMapping(configPayload) {
  const out = {};
  const locations = Array.isArray(configPayload.locations) ? configPayload.locations : [];

  locations.forEach((value) => {
    const key = String(value.key || "").trim().toLowerCase();
    if (!key) {
      return;
    }

    out[key] = {
      key,
      station: String(value.station || "").trim(),
      timezone: String(value.timezone || "").trim(),
      utc_offset_minutes:
        value.utc_offset_minutes === null || value.utc_offset_minutes === undefined
          ? null
          : Number(value.utc_offset_minutes),
      source: normalizeSource(value),
    };
  });

  return out;
}

async function loadConfig() {
  const response = await fetch("./config.json");
  if (!response.ok) {
    throw new Error(`Config load failed with HTTP ${response.status}`);
  }
  const payload = await response.json();
  return payload && typeof payload === "object" ? payload : {};
}

function parseJsonList(raw) {
  if (!raw) {
    return [];
  }
  if (Array.isArray(raw)) {
    return raw;
  }
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      return [];
    }
  }
  return [];
}

function extractFirstNumber(text) {
  const match = String(text).match(/-?\d+(?:\.\d+)?/);
  if (!match) {
    return Number.POSITIVE_INFINITY;
  }
  return Number(match[0]);
}

function selectionSortKey(selection) {
  const normalized = String(selection || "").trim().toLowerCase();
  let category = 1;
  if (normalized.includes("or below")) {
    category = 0;
  } else if (normalized.includes("or higher")) {
    category = 2;
  }
  return [category, extractFirstNumber(normalized), normalized];
}

function isUnbuyablePrice(value) {
  const asFloat = Number(value);
  if (!Number.isFinite(asFloat)) {
    return false;
  }
  return Math.abs(asFloat - 0.9995) < 1e-12;
}

function parseMonthNumber(value) {
  const cleaned = String(value || "").trim();
  if (!cleaned) {
    return null;
  }
  const date = new Date(`${cleaned} 1, 2000`);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.getMonth() + 1;
}

function extractEventDateOrdinal(eventTitle, eventSlug) {
  const title = String(eventTitle || "").trim();
  const slug = String(eventSlug || "").trim().toLowerCase();

  let match = EVENT_DATE_IN_TITLE_RE.exec(title);
  if (match) {
    const month = parseMonthNumber(match[1]);
    const day = Number(match[2]);
    const year = Number(match[3]);
    if (month) {
      const d = new Date(Date.UTC(year, month - 1, day));
      if (!Number.isNaN(d.getTime())) {
        return Math.floor(d.getTime() / 86400000);
      }
    }
  }

  match = EVENT_DATE_IN_SLUG_RE.exec(slug);
  if (match) {
    const month = parseMonthNumber(match[1]);
    const day = Number(match[2]);
    const year = Number(match[3]);
    if (month) {
      const d = new Date(Date.UTC(year, month - 1, day));
      if (!Number.isNaN(d.getTime())) {
        return Math.floor(d.getTime() / 86400000);
      }
    }
  }

  return null;
}

function eventLocationKey(eventSlug) {
  const slug = String(eventSlug || "").trim().toLowerCase();
  const match = EVENT_SLUG_RE.exec(slug);
  return match ? match[1] : "";
}

function parsePriceToCents(value) {
  const asFloat = Number(value);
  if (!Number.isFinite(asFloat)) {
    return "-";
  }
  return `${(asFloat * 100).toFixed(1)}c`;
}

function formatIntVolume(value) {
  if (typeof value === "boolean" || value === null || value === undefined) {
    return "-";
  }
  const asFloat = Number(value);
  if (!Number.isFinite(asFloat)) {
    return String(value);
  }
  return Math.round(asFloat).toLocaleString("en-US");
}

function preferredWeatherUnits(locationCfg) {
  if (!locationCfg) {
    return "m";
  }
  const accuweather = String(locationCfg.source && locationCfg.source.accuweather ? locationCfg.source.accuweather : "").toLowerCase();
  return accuweather.includes("/en/us/") ? "e" : "m";
}

function getOffsetMinutesFromTimeZone(timeZone) {
  try {
    const now = new Date();
    const utc = new Date(now.toLocaleString("en-US", { timeZone: "UTC" }));
    const zoned = new Date(now.toLocaleString("en-US", { timeZone }));
    return Math.round((zoned.getTime() - utc.getTime()) / 60000);
  } catch (error) {
    return null;
  }
}

function buildLocalTimeNow(locationKey, mapping) {
  const info = mapping[locationKey];
  if (!info) {
    return null;
  }

  if (info.timezone) {
    const offset = getOffsetMinutesFromTimeZone(info.timezone);
    return {
      timezone: info.timezone,
      utc_offset_minutes: offset,
      display: formatLocalTimeByTimeZone(info.timezone),
    };
  }

  if (typeof info.utc_offset_minutes === "number") {
    return {
      timezone: "",
      utc_offset_minutes: info.utc_offset_minutes,
      display: formatLocalTimeByOffset(info.utc_offset_minutes),
    };
  }

  return null;
}

function formatLocalTimeByTimeZone(timeZone) {
  try {
    const formatter = new Intl.DateTimeFormat("en-US", {
      timeZone,
      month: "2-digit",
      day: "2-digit",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    });
    return formatter.format(new Date()).replace(",", "");
  } catch (error) {
    return "-";
  }
}

function formatLocalTimeByOffset(offsetMinutes) {
  const nowUtcMs = Date.now() + new Date().getTimezoneOffset() * 60000;
  const localMs = nowUtcMs + offsetMinutes * 60000;
  const date = new Date(localMs);
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  let hour = date.getUTCHours();
  const minute = String(date.getUTCMinutes()).padStart(2, "0");
  const suffix = hour >= 12 ? "PM" : "AM";
  hour = hour % 12;
  if (hour === 0) {
    hour = 12;
  }
  return `${month}-${day} ${hour}:${minute}${suffix}`;
}

function localOffsetSortValue(localTimeNow) {
  if (!localTimeNow || typeof localTimeNow.utc_offset_minutes !== "number") {
    return null;
  }
  return localTimeNow.utc_offset_minutes;
}

function eventGroupSortComparator(a, b) {
  const aDate = Number.isInteger(a.event_date_sort_ordinal) ? a.event_date_sort_ordinal : 0;
  const bDate = Number.isInteger(b.event_date_sort_ordinal) ? b.event_date_sort_ordinal : 0;
  const aMissing = Number.isInteger(a.event_date_sort_ordinal) ? 0 : 1;
  const bMissing = Number.isInteger(b.event_date_sort_ordinal) ? 0 : 1;

  if (aMissing !== bMissing) {
    return aMissing - bMissing;
  }
  if (aDate !== bDate) {
    return aDate - bDate;
  }

  const aOffset = localOffsetSortValue(a.local_time_now);
  const bOffset = localOffsetSortValue(b.local_time_now);

  const aOffsetMissing = aOffset === null ? 1 : 0;
  const bOffsetMissing = bOffset === null ? 1 : 0;

  if (aOffsetMissing !== bOffsetMissing) {
    return aOffsetMissing - bOffsetMissing;
  }
  if (aOffset !== bOffset) {
    return (bOffset || 0) - (aOffset || 0);
  }

  return String(a.event_title || "").localeCompare(String(b.event_title || ""));
}

function buildEventGroups(payload, mapping) {
  const groupsMap = {};
  const localTimeCache = {};

  (payload.markets || []).forEach((market) => {
    if (market.closed) {
      return;
    }
    if (market.active !== undefined && market.active !== null && !market.active) {
      return;
    }

    const eventSlug = String(market.event_slug || "");
    const eventKey = eventSlug || String(market.event_id || market.id || "");
    const eventTitle = market.event_title || "Unknown event";
    const eventUrl = eventSlug ? `https://polymarket.com/event/${eventSlug}` : "-";
    const endDate = market.endDate || "-";
    const locationKey = eventLocationKey(eventSlug);
    const locationCfg = mapping[locationKey] || null;

    const hasLocationConfig = Boolean(locationCfg);
    const source = locationCfg ? { ...locationCfg.source } : {};
    const timezone = locationCfg ? locationCfg.timezone : "";
    const stationCode = locationCfg ? locationCfg.station : "";
    const weatherUnits = preferredWeatherUnits(locationCfg);

    if (!(locationKey in localTimeCache)) {
      localTimeCache[locationKey] = buildLocalTimeNow(locationKey, mapping);
    }
    const localTimeNow = localTimeCache[locationKey];

    if (!groupsMap[eventKey]) {
      groupsMap[eventKey] = {
        event_title: eventTitle,
        event_slug: eventSlug,
        event_url: eventUrl,
        end_date: endDate,
        source,
        station_code: stationCode,
        weather_units: weatherUnits,
        timezone,
        local_time_now: localTimeNow,
        local_time_display: localTimeNow ? localTimeNow.display : "-",
        event_date_sort_ordinal: extractEventDateOrdinal(eventTitle, eventSlug),
        is_secondary: !hasLocationConfig,
        selections: [],
      };
    }

    const outcomes = parseJsonList(market.outcomes);
    const outcomePrices = parseJsonList(market.outcomePrices);

    if (outcomePrices.some(isUnbuyablePrice)) {
      return;
    }

    const pairedOutcomes = [];
    const maxLen = Math.max(outcomes.length, outcomePrices.length);

    for (let i = 0; i < maxLen; i += 1) {
      pairedOutcomes.push({
        name: i < outcomes.length ? outcomes[i] : `Outcome ${i + 1}`,
        price: i < outcomePrices.length ? outcomePrices[i] : "-",
      });
    }

    let yesPrice = "-";
    let noPrice = "-";

    pairedOutcomes.forEach((outcome) => {
      const name = String(outcome.name || "").trim().toLowerCase();
      if (name === "yes") {
        yesPrice = outcome.price;
      } else if (name === "no") {
        noPrice = outcome.price;
      }
    });

    groupsMap[eventKey].selections.push({
      selection: market.groupItemTitle || market.question || "-",
      volume: formatIntVolume(market.volumeNum !== undefined ? market.volumeNum : market.volume),
      yes_price: parsePriceToCents(yesPrice),
      no_price: parsePriceToCents(noPrice),
    });
  });

  const groups = Object.values(groupsMap).filter((group) => group.selections.length > 0);

  groups.forEach((group) => {
    group.selections.sort((a, b) => {
      const aKey = selectionSortKey(a.selection);
      const bKey = selectionSortKey(b.selection);
      if (aKey[0] !== bKey[0]) {
        return aKey[0] - bKey[0];
      }
      if (aKey[1] !== bKey[1]) {
        return aKey[1] - bKey[1];
      }
      return String(aKey[2]).localeCompare(String(bKey[2]));
    });
  });

  groups.sort(eventGroupSortComparator);
  return groups;
}

function escapeHtml(raw) {
  return String(raw)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderGroups(eventGroups) {
  const groupsEl = document.getElementById("groups");
  const marketCountEl = document.getElementById("marketCount");
  const resultEl = document.getElementById("result");

  const marketCount = eventGroups.reduce((acc, g) => acc + g.selections.length, 0);
  marketCountEl.textContent = String(marketCount);

  const html = eventGroups
    .map((group) => {
      const sourceLinks = Object.entries(group.source || {})
        .map(([name, url]) => {
          if (!url) {
            return "";
          }
          const display = name.replace(/_/g, " ");
          return `<a class="btn-link" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer" data-bg-open="1">Open ${escapeHtml(display)}</a>`;
        })
        .join("");

      const selections = group.selections
        .map(
          (s) => `
            <tr>
              <td><div>${escapeHtml(s.selection)}</div></td>
              <td>${escapeHtml(s.yes_price)}</td>
              <td>${escapeHtml(s.no_price)}</td>
              <td>${escapeHtml(s.volume)}</td>
            </tr>
          `
        )
        .join("");

      return `
        <div class="event-card${group.is_secondary ? " is-secondary" : ""}" data-station-code="${escapeHtml(group.station_code)}" data-weather-units="${escapeHtml(group.weather_units)}" data-timezone="${escapeHtml(group.timezone)}">
          <div class="event-head">
            <div>
              <div class="event-title">${escapeHtml(group.event_title)}</div>
              <div class="links">
                <a class="btn-link ${group.event_url === "-" ? "is-disabled" : ""}" href="${group.event_url === "-" ? "" : escapeHtml(group.event_url)}" target="_blank" rel="noopener noreferrer" data-bg-open="1">Open event</a>
                ${sourceLinks}
              </div>
              <div class="event-meta weather-meta">
                <span data-weather-current>Current: --</span>
                <span> | </span>
                <span data-weather-max>Max: --</span>
              </div>
            </div>
            <div class="live-time">${escapeHtml(group.local_time_display)}</div>
          </div>

          <div class="graph-wrap" data-temp-graph></div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Selection</th>
                  <th>Yes</th>
                  <th>No</th>
                  <th>Volume</th>
                </tr>
              </thead>
              <tbody>
                ${selections}
              </tbody>
            </table>
          </div>
        </div>
      `;
    })
    .join("");

  groupsEl.innerHTML = html;
  resultEl.classList.remove("hidden");
}

function installBackgroundOpenBehavior() {
  document.addEventListener("click", (event) => {
    const link = event.target.closest("a[data-bg-open='1']");
    if (!link || !link.href || link.classList.contains("is-disabled")) {
      return;
    }

    event.preventDefault();
    const newTab = window.open(link.href, "_blank", "noopener,noreferrer");
    if (newTab) {
      try {
        newTab.opener = null;
      } catch (error) {
        // no-op
      }
      try {
        newTab.blur();
      } catch (error) {
        // no-op
      }
      try {
        window.focus();
      } catch (error) {
        // no-op
      }
    }
  });
}

function formatTemp(value, units) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "--";
  }
  return `${Math.round(number)}${units === "e" ? "°F" : "°C"}`;
}

function loadStationTemperatures() {
  const cards = document.querySelectorAll(".event-card[data-station-code]");

  cards.forEach((card) => {
    const stationCode = String(card.getAttribute("data-station-code") || "").trim();
    let units = String(card.getAttribute("data-weather-units") || "m").trim().toLowerCase();
    const currentEl = card.querySelector("[data-weather-current]");
    const maxEl = card.querySelector("[data-weather-max]");

    if (!stationCode || !currentEl || !maxEl) {
      return;
    }

    if (units !== "e" && units !== "m") {
      units = "m";
    }

    const url = `${WEATHER_CURRENT_URL}?${toSearchParams({
      language: "en-US",
      units,
      format: "json",
      icaoCode: stationCode,
    })}`;

    fetchExternalJson(url, "Weather API failed")
      .then((data) => {
        currentEl.textContent = `Current: ${formatTemp(data.temperature, units)}`;
        maxEl.textContent = `Max: ${formatTemp(data.temperatureMaxSince7Am, units)}`;
      })
      .catch(() => {
        currentEl.textContent = "Current: n/a";
        maxEl.textContent = "Max: n/a";
      });
  });
}

async function fetchMETARHistory(stationCode) {
  const url = `${METAR_HISTORY_URL}?ids=${encodeURIComponent(stationCode)}&hours=24&format=json`;
  return fetchExternalJson(url, "METAR history failed");
}

function parseMETARReadings(data, units) {
  const readings = [];
  (Array.isArray(data) ? data : []).forEach((obs) => {
    const tempC = obs.temp;
    const obsTime = obs.obsTime;
    if (tempC == null || obsTime == null) return;
    const temp = units === "e"
      ? parseFloat(tempC) * 9.0 / 5.0 + 32.0
      : parseFloat(tempC);
    readings.push({ time: parseInt(obsTime, 10) * 1000, temperature: Math.round(temp * 10) / 10 });
  });
  readings.sort((a, b) => a.time - b.time);
  return readings;
}

function formatTimeLabel(timestampMs, timezone) {
  const opts = { hour: "numeric", minute: "2-digit", hour12: true };
  try {
    if (timezone) {
      return new Intl.DateTimeFormat("en-US", { timeZone: timezone, ...opts }).format(new Date(timestampMs));
    }
  } catch (e) {
    // fall through
  }
  return new Date(timestampMs).toLocaleTimeString("en-US", opts);
}

function renderTemperatureGraph(readings, units, stationCode, timezone) {
  if (readings.length < 2) return "";

  const VW = 500, VH = 90;
  const PAD = { top: 10, right: 48, bottom: 18, left: 32 };
  const innerW = VW - PAD.left - PAD.right;
  const innerH = VH - PAD.top - PAD.bottom;

  const temps = readings.map((r) => r.temperature);
  const minTemp = Math.min(...temps);
  const maxTemp = Math.max(...temps);
  const range = maxTemp - minTemp || 1;

  const times = readings.map((r) => r.time);
  const minTime = times[0];
  const maxTime = times[times.length - 1];
  const timeSpan = maxTime - minTime || 1;

  const px = (t) => PAD.left + ((t - minTime) / timeSpan) * innerW;
  const py = (v) => PAD.top + innerH - ((v - minTemp) / range) * innerH;

  const pts = readings.map((r) => `${px(r.time).toFixed(1)},${py(r.temperature).toFixed(1)}`);
  const linePath = `M ${pts.join(" L ")}`;
  const baseY = (PAD.top + innerH).toFixed(1);
  const fillPath = `M ${px(minTime).toFixed(1)},${baseY} L ${pts.join(" L ")} L ${px(maxTime).toFixed(1)},${baseY} Z`;

  const last = readings[readings.length - 1];
  const lx = px(last.time).toFixed(1);
  const ly = py(last.temperature).toFixed(1);

  const unitSuffix = units === "e" ? "\u00b0F" : "\u00b0C";
  const gradId = `tg-${stationCode.replace(/[^a-zA-Z0-9]/g, "")}`;

  return `<svg class="temp-graph" viewBox="0 0 ${VW} ${VH}" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <linearGradient id="${gradId}" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="#3b82f6" stop-opacity="0.2"/>
        <stop offset="100%" stop-color="#3b82f6" stop-opacity="0"/>
      </linearGradient>
    </defs>
    <line x1="${PAD.left}" y1="${PAD.top}" x2="${PAD.left}" y2="${PAD.top + innerH}" stroke="#e2e8f0" stroke-width="1"/>
    <line x1="${PAD.left}" y1="${PAD.top + innerH}" x2="${PAD.left + innerW}" y2="${PAD.top + innerH}" stroke="#e2e8f0" stroke-width="1"/>
    <path d="${fillPath}" fill="url(#${gradId})"/>
    <path d="${linePath}" fill="none" stroke="#3b82f6" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>
    <circle cx="${lx}" cy="${ly}" r="3" fill="#3b82f6"/>
    <text x="${(parseFloat(lx) + 5).toFixed(1)}" y="${(parseFloat(ly) + 4).toFixed(1)}" font-size="11" fill="#3b82f6" font-weight="600">${escapeHtml(Math.round(last.temperature) + unitSuffix)}</text>
    <text x="${PAD.left - 3}" y="${(PAD.top + 4).toFixed(1)}" font-size="9" fill="#94a3b8" text-anchor="end">${escapeHtml(Math.round(maxTemp) + unitSuffix)}</text>
    <text x="${PAD.left - 3}" y="${(PAD.top + innerH).toFixed(1)}" font-size="9" fill="#94a3b8" text-anchor="end">${escapeHtml(Math.round(minTemp) + unitSuffix)}</text>
    <text x="${PAD.left}" y="${(VH - 2).toFixed(1)}" font-size="9" fill="#94a3b8">${escapeHtml(formatTimeLabel(minTime, timezone))}</text>
    <text x="${(PAD.left + innerW).toFixed(1)}" y="${(VH - 2).toFixed(1)}" font-size="9" fill="#94a3b8" text-anchor="end">${escapeHtml(formatTimeLabel(maxTime, timezone))}</text>
  </svg>`;
}

function loadTemperatureGraphs() {
  const cards = document.querySelectorAll(".event-card[data-station-code]");
  const stationToCards = {};

  cards.forEach((card) => {
    const code = String(card.getAttribute("data-station-code") || "").trim();
    if (!code) return;
    if (!stationToCards[code]) stationToCards[code] = [];
    stationToCards[code].push(card);
  });

  Object.entries(stationToCards).forEach(([code, cardList]) => {
    const units = String(cardList[0].getAttribute("data-weather-units") || "m").trim().toLowerCase();
    const timezone = String(cardList[0].getAttribute("data-timezone") || "").trim();
    fetchMETARHistory(code)
      .then((data) => {
        const readings = parseMETARReadings(data, units);
        const svgHtml = renderTemperatureGraph(readings, units, code, timezone);
        cardList.forEach((card) => {
          const el = card.querySelector("[data-temp-graph]");
          if (el) el.innerHTML = svgHtml;
        });
      })
      .catch(() => {});
  });
}

function showError(message) {
  const errorEl = document.getElementById("error");
  errorEl.textContent = message;
  errorEl.classList.remove("hidden");
}

async function init() {
  installBackgroundOpenBehavior();

  try {
    const configPayload = await loadConfig();
    const mapping = loadLocationMapping(configPayload);
    const payload = await fetchTemperatureMarketsPayload();
    const eventGroups = buildEventGroups(payload, mapping);
    renderGroups(eventGroups);
    loadStationTemperatures();
    loadTemperatureGraphs();
  } catch (error) {
    showError(`API error: ${error.message || String(error)}`);
  }
}

init();
