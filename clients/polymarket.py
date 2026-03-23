from __future__ import annotations

import json

import requests

from data.dto import ApiMarket, PublicSearchRequest, PublicSearchResponse

GAMMA_SEARCH_URL = "https://gamma-api.polymarket.com/public-search"
REQUEST_TIMEOUT_SECONDS = 20


def fetch_search_page(request_dto: PublicSearchRequest) -> PublicSearchResponse:
    params = request_dto.to_params()
    print(f"Sending request: GET {GAMMA_SEARCH_URL} params={params}")
    response = requests.get(GAMMA_SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    raw_payload = response.json()
    events_count = 0
    if isinstance(raw_payload, dict):
        events = raw_payload.get("events")
        if isinstance(events, list):
            events_count = len(events)
    print(f"Received response: status={response.status_code} events={events_count}")
    if not isinstance(raw_payload, dict):
        return PublicSearchResponse()
    return PublicSearchResponse.from_dict(raw_payload)


def looks_like_highest_temperature_market(market: ApiMarket) -> bool:
    text_parts = [
        str(market.question or ""),
        str(market.description or ""),
        str(market.slug or ""),
    ]
    haystack = " ".join(text_parts).lower()
    target_phrases = (
        "highest temperature",
        "highest temp",
        "daily high temperature",
        "daily high temp",
        "high temperature",
        "high temp",
        "hottest",
        "max temperature",
        "maximum temperature",
    )
    return any(phrase in haystack for phrase in target_phrases)


def fetch_markets(
    page_limit: int,
    max_pages: int,
    include_closed: bool,
    query: str,
) -> list[ApiMarket]:
    include_closed = False
    markets: list[ApiMarket] = []
    page = 1
    page_count = 0

    while True:
        if max_pages > 0 and page_count >= max_pages:
            break

        request_dto = PublicSearchRequest(
            query=query,
            limit_per_type=page_limit,
            page=page,
            search_tags=True,
            keep_closed_markets=include_closed,
        )
        response_dto = fetch_search_page(request_dto)
        page_markets: list[ApiMarket] = []
        for event in response_dto.events:
            for market in event.markets:
                market.event_id = event.id
                market.event_title = event.title
                market.event_slug = event.slug
                page_markets.append(market)

        if not page_markets:
            break

        markets.extend(page_markets)
        page_count += 1
        if len(response_dto.events) < page_limit:
            break
        page += 1

    return markets
