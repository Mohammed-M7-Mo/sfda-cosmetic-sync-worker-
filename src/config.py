"""Configuration loader and canonical SFDA cosmetic list definitions."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class ListTarget:
    slug: str
    source_nid: int
    name_ar: str
    url: str


SFDA_BASE_URL = "https://www.sfda.gov.sa"
SFDA_ALL_COSMETIC_NID = 17590

# Exact URLs and Drupal node IDs verified during Stage 1 discovery.
SFDA_LIST_TARGETS: tuple[ListTarget, ...] = (
    ListTarget(
        slug="prohibited",
        source_nid=17589,
        name_ar="قائمة المكونات الممنوع استخدامها في منتجات التجميل",
        url=f"{SFDA_BASE_URL}/ar/ProhibitedIngredientsList",
    ),
    ListTarget(
        slug="restricted",
        source_nid=17588,
        name_ar="قائمة المكونات المقيدة في منتجات التجميل",
        url=f"{SFDA_BASE_URL}/ar/RestrictedSubstancesList",
    ),
    ListTarget(
        slug="colorants",
        source_nid=17587,
        name_ar="قائمة المواد الملونة المسموح باستخدامها في منتجات التجميل",
        url=f"{SFDA_BASE_URL}/ar/AllowedColorants",
    ),
    ListTarget(
        slug="preservatives",
        source_nid=17586,
        name_ar="قائمة المواد الحافظة المسموح باستخدامها في منتجات التجميل",
        url=f"{SFDA_BASE_URL}/ar/AllowedPreservatives",
    ),
    ListTarget(
        slug="uv_filters",
        source_nid=17585,
        name_ar="قائمة المرشحات فوق البنفسجية المسموح باستخدامها في منتجات التجميل",
        url=f"{SFDA_BASE_URL}/ar/AllowedUVFilters",
    ),
)

SFDA_TARGETS_BY_SLUG: dict[str, ListTarget] = {t.slug: t for t in SFDA_LIST_TARGETS}

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


def _parse_bool(raw: str | None, default: bool) -> bool:
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class WorkerConfig:
    wp_sync_base_url: str
    wp_api_key: str
    wp_hmac_secret: str
    lists_scope: tuple[str, ...]
    request_delay_sec: float
    detail_concurrency: int
    http_timeout_sec: float
    max_retries: int
    batch_size: int
    enable_enrichment: bool
    checkpoint_ttl_hours: int
    state_dir: Path
    trigger_source: str
    dry_run: bool
    force_commit: bool
    max_pages_per_list: int | None = None

    @property
    def scope_label(self) -> str:
        all_slugs = tuple(t.slug for t in SFDA_LIST_TARGETS)
        if self.lists_scope == all_slugs:
            return "all"
        return ",".join(self.lists_scope)

    @classmethod
    def from_env(
        cls,
        *,
        lists_scope_override: str | None = None,
        dry_run: bool = False,
        force_commit: bool = False,
        max_pages_per_list: int | None = None,
        trigger_source: str | None = None,
    ) -> WorkerConfig:
        load_dotenv()

        raw_scope = (lists_scope_override or os.getenv("SFDA_LISTS_SCOPE") or "all").strip().lower()
        if raw_scope in ("", "all"):
            resolved_scope = tuple(t.slug for t in SFDA_LIST_TARGETS)
        else:
            parsed = [s.strip() for s in raw_scope.split(",") if s.strip()]
            invalid = [s for s in parsed if s not in SFDA_TARGETS_BY_SLUG]
            if invalid:
                raise ValueError(
                    f"Invalid list slug(s): {invalid}. Valid slugs: {list(SFDA_TARGETS_BY_SLUG.keys())}"
                )
            # Preserve canonical ordering without duplicates
            resolved_scope = tuple(t.slug for t in SFDA_LIST_TARGETS if t.slug in parsed)

        wp_base = (os.getenv("WP_SYNC_BASE_URL") or "").strip().rstrip("/")
        wp_key = (os.getenv("WP_SFDA_API_KEY") or "").strip()
        wp_secret = (os.getenv("WP_SFDA_HMAC_SECRET") or "").strip()

        if not dry_run and (not wp_base or not wp_key or not wp_secret):
            raise ValueError(
                "Missing required WordPress sync credentials: "
                "WP_SYNC_BASE_URL, WP_SFDA_API_KEY, and WP_SFDA_HMAC_SECRET must be set (or run with --dry-run)."
            )

        detected_trigger = trigger_source or (
            "github_actions" if os.getenv("GITHUB_ACTIONS") == "true" else "manual_cli"
        )

        return cls(
            wp_sync_base_url=wp_base,
            wp_api_key=wp_key,
            wp_hmac_secret=wp_secret,
            lists_scope=resolved_scope,
            request_delay_sec=max(0.1, float(os.getenv("SFDA_REQUEST_DELAY_SEC", "0.45"))),
            detail_concurrency=max(1, min(6, int(os.getenv("SFDA_DETAIL_CONCURRENCY", "3")))),
            http_timeout_sec=max(10.0, float(os.getenv("SFDA_HTTP_TIMEOUT_SEC", "45"))),
            max_retries=max(1, min(10, int(os.getenv("SFDA_MAX_RETRIES", "5")))),
            batch_size=max(10, min(250, int(os.getenv("SFDA_BATCH_SIZE", "100")))),
            enable_enrichment=_parse_bool(os.getenv("SFDA_ENABLE_ENRICHMENT"), default=True),
            checkpoint_ttl_hours=max(1, int(os.getenv("SFDA_CHECKPOINT_TTL_HOURS", "36"))),
            state_dir=Path(os.getenv("SFDA_STATE_DIR", "state")).resolve(),
            trigger_source=detected_trigger,
            dry_run=dry_run,
            force_commit=force_commit,
            max_pages_per_list=max_pages_per_list,
        )
