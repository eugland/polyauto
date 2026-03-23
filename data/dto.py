from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _clean_dict(data: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in data.items() if v is not None}


@dataclass(frozen=True)
class PublicSearchRequest:
    query: str
    limit_per_type: int
    page: int
    search_tags: bool = True
    keep_closed_markets: bool = False

    def to_params(self) -> dict[str, Any]:
        return {
            "q": self.query,
            "limit_per_type": self.limit_per_type,
            "page": self.page,
            "search_tags": "true" if self.search_tags else "false",
            "keep_closed_markets": 1 if self.keep_closed_markets else 0,
        }


@dataclass
class ApiTag:
    id: str | None = None
    label: str | None = None
    slug: str | None = None
    forceShow: bool | None = None
    publishedAt: str | None = None
    createdAt: str | None = None
    updatedAt: str | None = None
    isCarousel: bool | None = None
    requiresTranslation: bool | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ApiTag":
        return cls(
            id=str(raw.get("id")) if raw.get("id") is not None else None,
            label=raw.get("label"),
            slug=raw.get("slug"),
            forceShow=raw.get("forceShow"),
            publishedAt=raw.get("publishedAt"),
            createdAt=raw.get("createdAt"),
            updatedAt=raw.get("updatedAt"),
            isCarousel=raw.get("isCarousel"),
            requiresTranslation=raw.get("requiresTranslation"),
        )

    def to_dict(self) -> dict[str, Any]:
        return _clean_dict(
            {
                "id": self.id,
                "label": self.label,
                "slug": self.slug,
                "forceShow": self.forceShow,
                "publishedAt": self.publishedAt,
                "createdAt": self.createdAt,
                "updatedAt": self.updatedAt,
                "isCarousel": self.isCarousel,
                "requiresTranslation": self.requiresTranslation,
            }
        )


@dataclass
class ApiMarket:
    id: str | None = None
    question: str | None = None
    conditionId: str | None = None
    slug: str | None = None
    resolutionSource: str | None = None
    endDate: str | None = None
    startDate: str | None = None
    image: str | None = None
    icon: str | None = None
    description: str | None = None
    outcomes: str | None = None
    outcomePrices: str | None = None
    volume: str | None = None
    active: bool | None = None
    closed: bool | None = None
    marketMakerAddress: str | None = None
    createdAt: str | None = None
    updatedAt: str | None = None
    closedTime: str | None = None
    new: bool | None = None
    featured: bool | None = None
    submitted_by: str | None = None
    archived: bool | None = None
    resolvedBy: str | None = None
    restricted: bool | None = None
    groupItemTitle: str | None = None
    groupItemThreshold: str | None = None
    questionID: str | None = None
    umaEndDate: str | None = None
    enableOrderBook: bool | None = None
    orderPriceMinTickSize: float | int | None = None
    orderMinSize: float | int | None = None
    umaResolutionStatus: str | None = None
    volumeNum: float | int | None = None
    liquidityNum: float | int | None = None
    endDateIso: str | None = None
    startDateIso: str | None = None
    hasReviewedDates: bool | None = None
    volume24hr: float | int | None = None
    volume1wk: float | int | None = None
    volume1mo: float | int | None = None
    volume1yr: float | int | None = None
    gameStartTime: str | None = None
    secondsDelay: float | int | None = None
    clobTokenIds: str | None = None
    umaBond: str | None = None
    umaReward: str | None = None
    volume24hrClob: float | int | None = None
    volume1wkClob: float | int | None = None
    volume1moClob: float | int | None = None
    volume1yrClob: float | int | None = None
    volumeClob: float | int | None = None
    liquidityAmm: float | int | None = None
    liquidityClob: float | int | None = None
    customLiveness: float | int | None = None
    acceptingOrders: bool | None = None
    negRisk: bool | None = None
    negRiskMarketID: str | None = None
    negRiskRequestID: str | None = None
    ready: bool | None = None
    funded: bool | None = None
    acceptingOrdersTimestamp: str | None = None
    cyom: bool | None = None
    competitive: float | int | None = None
    pagerDutyNotificationEnabled: bool | None = None
    approved: bool | None = None
    rewardsMinSize: float | int | None = None
    rewardsMaxSpread: float | int | None = None
    spread: float | int | None = None
    automaticallyResolved: bool | None = None
    lastTradePrice: float | int | None = None
    bestAsk: float | int | None = None
    automaticallyActive: bool | None = None
    clearBookOnStart: bool | None = None
    manualActivation: bool | None = None
    negRiskOther: bool | None = None
    umaResolutionStatuses: str | None = None
    pendingDeployment: bool | None = None
    deploying: bool | None = None
    deployingTimestamp: str | None = None
    rfqEnabled: bool | None = None
    holdingRewardsEnabled: bool | None = None
    feesEnabled: bool | None = None
    requiresTranslation: bool | None = None
    feeType: str | None = None
    # derived event context for UI grouping
    event_id: str | None = None
    event_title: str | None = None
    event_slug: str | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ApiMarket":
        return cls(**{k: raw.get(k) for k in cls.__dataclass_fields__.keys()})

    def to_dict(self) -> dict[str, Any]:
        return _clean_dict({k: getattr(self, k) for k in self.__dataclass_fields__})


@dataclass
class EventMetadata:
    context_description: str | None = None
    context_requires_regen: bool | None = None
    context_updated_at: str | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "EventMetadata":
        return cls(
            context_description=raw.get("context_description"),
            context_requires_regen=raw.get("context_requires_regen"),
            context_updated_at=raw.get("context_updated_at"),
        )

    def to_dict(self) -> dict[str, Any]:
        return _clean_dict(
            {
                "context_description": self.context_description,
                "context_requires_regen": self.context_requires_regen,
                "context_updated_at": self.context_updated_at,
            }
        )


@dataclass
class ApiEvent:
    id: str | None = None
    ticker: str | None = None
    slug: str | None = None
    title: str | None = None
    description: str | None = None
    resolutionSource: str | None = None
    startDate: str | None = None
    creationDate: str | None = None
    endDate: str | None = None
    image: str | None = None
    icon: str | None = None
    active: bool | None = None
    closed: bool | None = None
    archived: bool | None = None
    new: bool | None = None
    featured: bool | None = None
    restricted: bool | None = None
    liquidity: float | int | None = None
    volume: float | int | None = None
    openInterest: float | int | None = None
    createdAt: str | None = None
    updatedAt: str | None = None
    competitive: float | int | None = None
    volume24hr: float | int | None = None
    volume1wk: float | int | None = None
    volume1mo: float | int | None = None
    volume1yr: float | int | None = None
    enableOrderBook: bool | None = None
    liquidityClob: float | int | None = None
    negRisk: bool | None = None
    negRiskMarketID: str | None = None
    commentCount: int | None = None
    cyom: bool | None = None
    showAllOutcomes: bool | None = None
    showMarketImages: bool | None = None
    enableNegRisk: bool | None = None
    automaticallyActive: bool | None = None
    eventDate: str | None = None
    startTime: str | None = None
    seriesSlug: str | None = None
    negRiskAugmented: bool | None = None
    pendingDeployment: bool | None = None
    deploying: bool | None = None
    deployingTimestamp: str | None = None
    requiresTranslation: bool | None = None
    markets: list[ApiMarket] = field(default_factory=list)
    tags: list[ApiTag] = field(default_factory=list)
    eventMetadata: EventMetadata | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ApiEvent":
        raw_markets = raw.get("markets") or []
        raw_tags = raw.get("tags") or []
        metadata_raw = raw.get("eventMetadata")
        values = {k: raw.get(k) for k in cls.__dataclass_fields__.keys()}
        values["markets"] = [ApiMarket.from_dict(m) for m in raw_markets if isinstance(m, dict)]
        values["tags"] = [ApiTag.from_dict(t) for t in raw_tags if isinstance(t, dict)]
        values["eventMetadata"] = (
            EventMetadata.from_dict(metadata_raw)
            if isinstance(metadata_raw, dict)
            else None
        )
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        data = _clean_dict({k: getattr(self, k) for k in self.__dataclass_fields__})
        data["markets"] = [m.to_dict() for m in self.markets]
        data["tags"] = [t.to_dict() for t in self.tags]
        if self.eventMetadata is not None:
            data["eventMetadata"] = self.eventMetadata.to_dict()
        return data


@dataclass
class PublicSearchResponse:
    events: list[ApiEvent] = field(default_factory=list)
    profiles: list[dict[str, Any]] = field(default_factory=list)
    tags: list[ApiTag] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PublicSearchResponse":
        raw_events = raw.get("events") or []
        raw_tags = raw.get("tags") or []
        raw_profiles = raw.get("profiles") or []
        return cls(
            events=[ApiEvent.from_dict(e) for e in raw_events if isinstance(e, dict)],
            profiles=[p for p in raw_profiles if isinstance(p, dict)],
            tags=[ApiTag.from_dict(t) for t in raw_tags if isinstance(t, dict)],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "events": [e.to_dict() for e in self.events],
            "profiles": self.profiles,
            "tags": [t.to_dict() for t in self.tags],
        }


@dataclass
class MarketsOutput:
    query: str
    include_closed: bool
    market_count: int
    markets: list[ApiMarket]

    def to_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "include_closed": self.include_closed,
            "market_count": self.market_count,
            "markets": [m.to_dict() for m in self.markets],
        }

