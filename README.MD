# Polymarket Temperature Markets

A Flask web application that displays Polymarket temperature prediction markets alongside real-time and historical weather data from aviation weather stations worldwide.

---

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Configuration](#configuration)
- [API Reference](#api-reference)
  - [GET /](#get-)
  - [GET /api/weather/current](#get-apiweathercurrent)
  - [GET /api/weather/all](#get-apiweatherall)
  - [GET /api/weather/history](#get-apiweatherhistory)
  - [GET /api/weather/daily-high](#get-apiweatherdaily-high)
- [External APIs](#external-apis)
- [Environment Variables](#environment-variables)
- [Project Structure](#project-structure)

---

## Overview

The app fetches active temperature prediction markets from Polymarket (markets closing within 30 hours), pairs each market with live weather data from the corresponding ICAO weather station, and displays both together in a single dashboard. Weather data is cached for 10 minutes and refreshed in the background.

---

## Setup

```bash
pip install -r requirements.txt
python app.py
```

The server starts on `http://127.0.0.1:5000` by default (configurable in `webapp.json`).

---

## Configuration

**`webapp.json`** — primary configuration file.

```json
{
  "settings": {
    "run_host": "127.0.0.1",
    "run_port": 5000,
    "run_debug": true
  },
  "locations": [
    {
      "key": "seattle",
      "station": "KSEA",
      "timezone": "America/Los_Angeles",
      "utc_offset_minutes": -420,
      "source": {
        "wunderground": "https://www.wunderground.com/weather/KSEA"
      }
    }
  ]
}
```

| Field | Description |
|---|---|
| `settings.run_host` | Host to bind Flask to |
| `settings.run_port` | Port to bind Flask to |
| `settings.run_debug` | Enable Flask debug mode |
| `locations[].key` | Lowercase identifier used to match Polymarket slugs |
| `locations[].station` | ICAO code for the weather station |
| `locations[].timezone` | IANA timezone string |
| `locations[].utc_offset_minutes` | Fallback UTC offset in minutes |
| `locations[].source` | Map of data source names to their URLs |

36 locations are pre-configured: Seattle, London, Dallas, Chicago, Paris, São Paulo, Buenos Aires, Seoul, Toronto, New York, Atlanta, Miami, Ankara, Wellington, Lucknow, Munich, Tokyo, Shanghai, Singapore, Milan, Madrid, Warsaw, Austin, Denver, Houston, Los Angeles, San Francisco, Chongqing, Beijing, Wuhan, Shenzhen, Chengdu, Tel Aviv, Hong Kong, and Taipei.

---

## API Reference

### GET /

Renders the main HTML dashboard.

**Request Parameters:** None

**Response:** `text/html` — Jinja2-rendered page containing:
- Temperature market event groups grouped by location
- Total market count
- Error message if the Polymarket API call fails

**Status Codes:**

| Code | Meaning |
|---|---|
| 200 | Page rendered successfully |

---

### GET /api/weather/current

Fetches the current weather observation for a single ICAO station from the Weather.com API.

**Request Parameters (query string):**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `location_code` | string | Yes | ICAO station code (e.g. `KSEA`, `EGLC`). Also accepted as `icao_code`. Case-insensitive. Must be alphanumeric, 3–8 characters. |
| `units` | string | No | `"m"` for Celsius (default) or `"e"` for Fahrenheit |

**Example Request:**

```
GET /api/weather/current?location_code=KSEA&units=m
```

**Success Response — 200:**

Returns the raw JSON object from Weather.com:

```json
{
  "temperature": 12,
  "temperatureMaxSince7Am": 14,
  "obs_time": 1711500000,
  "...": "additional weather.com fields"
}
```

**Error Responses:**

| Code | Condition | Body |
|---|---|---|
| 400 | Missing `location_code` | `{"error": "location_code query parameter is required"}` |
| 400 | Invalid format | `{"error": "Invalid location_code format"}` |
| 400 | Invalid `units` value | `{"error": "units must be 'm' or 'e'"}` |
| 500 | `WEATHER_API_KEY` not set | `{"error": "WEATHER_API_KEY is not configured"}` |
| 502 | Weather.com request failed | `{"error": "...", "status_code": <number>, "details": "<raw response>"}` |

---

### GET /api/weather/all

Fetches 24-hour temperature history for **all** configured stations in parallel. Results are cached for 10 minutes; stale data triggers a non-blocking background refresh.

**Request Parameters:** None

**Example Request:**

```
GET /api/weather/all
```

**Success Response — 200:**

```json
{
  "KSEA": {
    "readings": [
      { "time_utc": "2024-03-27T12:00:00Z", "temperature": 10 },
      { "time_utc": "2024-03-27T11:00:00Z", "temperature": 9 }
    ],
    "daily_high": 14,
    "current": 12
  },
  "EGLC": {
    "readings": [...],
    "daily_high": 11,
    "current": 9
  }
}
```

| Field | Type | Description |
|---|---|---|
| `<STATION>` | object | Keyed by ICAO station code |
| `readings` | array | Hourly readings, newest first |
| `readings[].time_utc` | string | ISO 8601 UTC timestamp |
| `readings[].temperature` | number | Temperature in Celsius |
| `daily_high` | number | Highest reading in the past 24 hours |
| `current` | number | Most recent temperature reading |

**Error Responses:**

| Code | Condition |
|---|---|
| 503 | Cold start — cache not populated within 30 seconds |

**Caching Details:**

- Cache TTL: **600 seconds** (10 minutes)
- On cold start, the endpoint blocks up to 30 seconds for the initial fetch
- Subsequent stale requests return cached data immediately and refresh in the background
- Parallel fetches use a thread pool of 8 workers

---

### GET /api/weather/history

Fetches historical METAR temperature readings for a single station over a configurable window (up to 7 days).

**Request Parameters (query string):**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `icao_code` | string | Yes | ICAO station code (e.g. `KSEA`). Case-insensitive, alphanumeric, 3–8 characters. |
| `units` | string | No | `"m"` for Celsius (default) or `"e"` for Fahrenheit |
| `hours` | integer | No | Number of hours of history to fetch. Range: 1–168 (7 days). Default: 24. |

**Example Request:**

```
GET /api/weather/history?icao_code=KSEA&hours=48&units=m
```

**Success Response — 200:**

```json
{
  "icao_code": "KSEA",
  "units": "m",
  "hours": 48,
  "readings": [
    { "time_utc": "2024-03-27T12:00:00Z", "temperature": 10 },
    { "time_utc": "2024-03-27T11:00:00Z", "temperature": 9 }
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `icao_code` | string | The station code that was queried |
| `units` | string | Unit system used (`"m"` or `"e"`) |
| `hours` | number | Number of hours requested |
| `readings` | array | Hourly METAR readings |
| `readings[].time_utc` | string | ISO 8601 UTC timestamp |
| `readings[].temperature` | number | Temperature in the requested unit |

**Error Responses:**

| Code | Condition |
|---|---|
| 400 | Missing `icao_code` |
| 400 | Invalid `icao_code` format |
| 400 | Invalid `units` value |
| 400 | `hours` is not an integer |
| 400 | `hours` outside range 1–168 |
| 502 | METAR API request failed |

---

### GET /api/weather/daily-high

Returns the highest temperature reading for a station over the past 24 hours.

**Request Parameters (query string):**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `icao_code` | string | Yes | ICAO station code. Case-insensitive, alphanumeric, 3–8 characters. |
| `units` | string | No | `"m"` for Celsius (default) or `"e"` for Fahrenheit |

**Example Request:**

```
GET /api/weather/daily-high?icao_code=KSEA&units=m
```

**Success Response — 200:**

```json
{
  "icao_code": "KSEA",
  "units": "m",
  "daily_high": 14,
  "reading_count": 23
}
```

| Field | Type | Description |
|---|---|---|
| `icao_code` | string | The station code that was queried |
| `units` | string | Unit system used (`"m"` or `"e"`) |
| `daily_high` | number \| null | Highest temperature in the past 24 hours, or `null` if no data |
| `reading_count` | number | Number of METAR readings found |

**Error Responses:**

| Code | Condition |
|---|---|
| 400 | Missing `icao_code` |
| 400 | Invalid `icao_code` format |
| 400 | Invalid `units` value |
| 502 | METAR API request failed |

---

## External APIs

The application consumes three external APIs:

### Polymarket Gamma API

- **Endpoint:** `https://gamma-api.polymarket.com/events`
- **Method:** GET
- **Auth:** None
- **Parameters used:** `tag_slug=temperature`, `closed=false`, `limit=200`, `end_date_max=<ISO timestamp 30h from now>`
- **Purpose:** Fetch active temperature prediction markets

### Weather.com Observations API

- **Endpoint:** `https://api.weather.com/v3/wx/observations/current`
- **Method:** GET
- **Auth:** `apiKey` query parameter (set via `WEATHER_API_KEY` env var)
- **Parameters used:** `icaoCode`, `units`, `language=en-US`, `format=json`
- **Purpose:** Fetch current weather observations per station

### Aviation Weather Center METAR API

- **Endpoint:** `https://aviationweather.gov/api/data/metar`
- **Method:** GET
- **Auth:** None (public)
- **Parameters used:** `ids=<icao_code>`, `hours=<1-168>`, `format=json`
- **Purpose:** Fetch historical hourly METAR temperature readings

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `WEATHER_API_KEY` | No | API key for Weather.com. Falls back to a default key if not set. |

---

## Bet Viewer

`automata/view_bets.py` serves a local UI for inspecting and annotating `bets.db`.

```bash
python -m automata.view_bets
# → http://localhost:5050
```

| Feature | Detail |
|---|---|
| Summary bar | Total bets, cost, wins/losses/open, win rate |
| Table | All recorded bets, newest first |
| Filters | Filter by city or outcome (win / loss / open) |
| Inline edit | Set `resolved_temp` and `outcome` (win/loss) per row directly in the browser |
| JSON API | `GET /api/bets` — returns all rows as JSON for use in ML pipelines |

See [docs/running.md](docs/running.md) for how to run the automata bot and frontend.

---

## Project Structure

```
PythonProject/
├── app.py                          # Flask application entry point
├── webapp.json                     # Runtime config and location definitions
├── requirements.txt                # Python dependencies
├── controllers/
│   └── home_controller.py          # All route handlers
├── services/
│   ├── config_service.py           # Config parsing
│   ├── event_group_service.py      # Polymarket data transformation
│   ├── polymarket_service.py       # Polymarket API client
│   ├── timezone_service.py         # Timezone utilities
│   └── weather_history_service.py  # METAR API client + caching
├── domains/
│   ├── constants.py                # App-wide constants and regex patterns
│   └── models.py                   # LocationConfig, RuntimeSettings dataclasses
├── templates/
│   └── index.html                  # Jinja2 HTML template
├── bets.db                         # SQLite bet log (auto-created on first live run)
├── automata/
│   ├── main.py                     # Betting engine entry point
│   ├── client.py                   # Polymarket CLOB client helpers
│   ├── weather.py                  # Forecast + METAR fetchers
│   ├── parser.py                   # Market question parser
│   ├── db.py                       # bets.db schema + read/write helpers
│   └── view_bets.py                # Browser UI for bets.db (port 5050)
└── docs/
    ├── running.md                  # How to run automata and frontend
    ├── app.js                      # Standalone browser-only version
    ├── config.json                 # Config copy for standalone version
    └── index.html                  # Standalone HTML page
```
