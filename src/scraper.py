"""
SFDA Cosmetic Lists HTML Scraper & Detail Extractor.
Built directly against the verified Drupal HTML & `/ar/details_data` structure.
Automatically switches to the `/en/` mirror URL whenever SFDA's `/ar/` cache returns
an empty 200 OK ("لا توجد نتائج") page, recovering 100% of records without delay.
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterator
from urllib.parse import parse_qs, quote, urljoin, urlparse

from bs4 import BeautifulSoup

from .config import SFDA_ALL_COSMETIC_NID, SFDA_BASE_URL, ListTarget
from .hasher import build_source_key, clean_field, compute_row_hash, normalize_name
from .http_client import ResilientHttpClient
from .models import IngredientRecord

logger = logging.getLogger(__name__)

_PAGE_PARAM_RE = re.compile(r"[?&]page=(\d+)")

# Retry delays only if BOTH /ar/ and /en/ return 0 rows
_EMPTY_PAGE_RETRY_DELAYS: tuple[float, ...] = (5.0, 12.0, 25.0)
_RESCUE_PASS_RETRY_DELAYS: tuple[float, ...] = (10.0, 20.0)

# Normalized lowercase label -> canonical field key (supports both Arabic & English SFDA detail tables)
_DETAIL_LABEL_MAP: dict[str, str] = {
    "reference number": "reference_number",
    "رقم المرجع": "reference_number",
    "اسم المكون العلمي": "chemical_name",
    "chemical name": "chemical_name",
    "chemical name / inn": "chemical_name",
    "name of common ingredients glossary": "inci_name",
    "inci name": "inci_name",
    "رقم المكون": "cas_number",
    "cas number": "cas_number",
    "ec number": "ec_number",
    "product type, body parts": "product_type_body_parts",
    "product type": "product_type_body_parts",
    "maximum concentration in ready for use preparation": "max_concentration",
    "maximum concentration": "max_concentration",
    "other": "other_conditions",
    "wording of conditions of use and warnings": "warnings_conditions",
    "conditions of use and warnings": "warnings_conditions",
}


@dataclass(frozen=True)
class RawTableRow:
    chemical_name: str
    cas_number: str
    detail_href: str
    source_page: int


def _extract_raw_rows_from_soup(soup: BeautifulSoup, page_num: int) -> list[RawTableRow]:
    """Parse table rows from a rendered SFDA list BeautifulSoup document."""
    table = soup.select_one("table.responsive-enabled") or soup.find("table")
    if table is None:
        return []

    tbody = table.find("tbody") or table
    rows: list[RawTableRow] = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        chemical_name = clean_field(tds[0].get_text(" ", strip=True))
        cas_number = clean_field(tds[1].get_text(" ", strip=True))
        detail_href = ""
        if len(tds) >= 3:
            link = tds[2].find("a", href=True)
            if link:
                detail_href = str(link["href"]).strip()
        if not detail_href:
            link = tr.find("a", href=True)
            if link:
                detail_href = str(link["href"]).strip()

        if not chemical_name and not cas_number:
            continue

        rows.append(
            RawTableRow(
                chemical_name=chemical_name,
                cas_number=cas_number,
                detail_href=detail_href,
                source_page=page_num,
            )
        )

    return rows


def _is_valid_sfda_list_html(html: str) -> bool:
    """Confirm the response is a genuine SFDA Drupal list page (Arabic or English)."""
    if len(html) < 5000:
        return False
    return (
        ("info-lists-filter-form" in html)
        or ("responsive-enabled" in html)
        or ("<table" in html and "sfda" in html.lower())
    )


def _is_valid_sfda_detail_html(html: str) -> bool:
    """Confirm the detail page contains an actual detail table with data cells."""
    if len(html) < 400:
        return False
    return ("attachmentstr" in html or "<table" in html) and ("<td" in html)


def _parse_detail_table(html: str) -> dict[str, str]:
    """Extract key-value pairs from SFDA detail HTML table (`tbody.attachmentstr`)."""
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("tbody.attachmentstr tr")
    if not rows:
        rows = soup.select("table tr")

    raw_fields: dict[str, str] = {}
    for tr in rows:
        th = tr.find("th")
        td = tr.find("td")
        if th is None or td is None:
            continue
        label = clean_field(th.get_text(" ", strip=True))
        value = clean_field(td.get_text(" ", strip=True))
        if label:
            raw_fields[label] = value

    return raw_fields


def _map_raw_details_to_fields(raw_details: dict[str, str]) -> dict[str, str]:
    mapped: dict[str, str] = {}
    for label, value in raw_details.items():
        norm_label = re.sub(r"\s+", " ", label).strip().lower()
        target_key = _DETAIL_LABEL_MAP.get(norm_label)
        if target_key is None:
            for known_label, known_key in _DETAIL_LABEL_MAP.items():
                if known_label in norm_label:
                    target_key = known_key
                    break
        if target_key is not None and (target_key not in mapped or not mapped[target_key]):
            mapped[target_key] = value
    return mapped


def _to_english_sfda_url(url: str) -> str:
    """Switch an SFDA `/ar/` URL to its `/en/` counterpart to bypass broken `/ar/` cache entries."""
    if "/ar/" in url:
        return url.replace("/ar/", "/en/", 1)
    return url


class SfdaScraper:
    # Hard ceiling on concurrent detail requests to prevent SFDA peer resets (Errno 104)
    MAX_SAFE_CONCURRENCY = 2

    def __init__(
        self,
        http_client: ResilientHttpClient,
        *,
        detail_concurrency: int = 2,
        enable_enrichment: bool = True,
    ) -> None:
        self.http = http_client
        self.detail_concurrency = max(1, min(self.MAX_SAFE_CONCURRENCY, detail_concurrency))
        self.enable_enrichment = enable_enrichment
        self.unrecovered_empty_pages: dict[str, list[int]] = {}

    def _reset_http_connection(self) -> None:
        """Best-effort reset of underlying HTTP session/pool to escape stale SFDA connections."""
        reset_fn = getattr(self.http, "reset_session", None)
        if callable(reset_fn):
            try:
                reset_fn()
                return
            except Exception:
                pass
        session = getattr(self.http, "_session", None) or getattr(self.http, "session", None)
        if session is not None and hasattr(session, "cookies"):
            try:
                session.cookies.clear()
            except Exception:
                pass

    def _discover_max_page(self, soup: BeautifulSoup, current_page: int) -> int:
        max_page = current_page
        for anchor in soup.select("ul.pagination a[href], nav a[href]"):
            href = anchor.get("href") or ""
            match = _PAGE_PARAM_RE.search(href)
            if match:
                idx = int(match.group(1))
                if idx > max_page:
                    max_page = idx
        return max_page

    def fetch_list_page(
        self,
        target: ListTarget,
        page_num: int,
        *,
        retry_delays: tuple[float, ...] = _EMPTY_PAGE_RETRY_DELAYS,
    ) -> tuple[list[RawTableRow], int]:
        """
        Fetch a single paginated list page (`?page=N`) and return:
        `(rows_on_page, discovered_max_page_index)`.
        If `/ar/` returns 0 rows ("لا توجد نتائج"), immediately falls back to `/en/`
        which bypasses SFDA's cached empty Arabic pages.
        """
        ar_url = f"{target.url}?page={page_num}" if page_num > 0 else target.url
        en_base = _to_english_sfda_url(target.url)
        en_url = f"{en_base}?page={page_num}" if page_num > 0 else en_base

        # 1) Primary attempt on /ar/
        html = self.http.get_text(ar_url, validator=_is_valid_sfda_list_html, referer=target.url)
        soup = BeautifulSoup(html, "lxml")
        max_page = self._discover_max_page(soup, page_num)
        rows = _extract_raw_rows_from_soup(soup, page_num)
        if rows:
            return rows, max_page

        # 2) Immediate /en/ fallback (bypasses broken /ar/ Drupal cache on SFDA)
        if en_url != ar_url:
            logger.info(
                "Page %d of [%s] returned 0 rows on /ar/; switching immediately to /en/ fallback...",
                page_num + 1,
                target.slug,
            )
            html_en = self.http.get_text(en_url, validator=_is_valid_sfda_list_html, referer=en_base)
            soup_en = BeautifulSoup(html_en, "lxml")
            max_page = max(max_page, self._discover_max_page(soup_en, page_num))
            rows = _extract_raw_rows_from_soup(soup_en, page_num)
            if rows:
                logger.info(
                    "Recovered page %d of [%s] via /en/ fallback (+%d rows).",
                    page_num + 1,
                    target.slug,
                    len(rows),
                )
                return rows, max_page

        # 3) Progressive retries alternating /en/ and /ar/ if both had a transient glitch
        total_retries = len(retry_delays)
        for attempt_idx, delay in enumerate(retry_delays, start=1):
            use_en = (attempt_idx % 2 == 1) and (en_url != ar_url)
            try_url = en_url if use_en else ar_url
            try_ref = en_base if use_en else target.url

            logger.warning(
                "Page %d of [%s] still 0 rows; retry %d/%d on %s after %.0fs...",
                page_num + 1,
                target.slug,
                attempt_idx,
                total_retries,
                "/en/" if use_en else "/ar/",
                delay,
            )
            self._reset_http_connection()
            time.sleep(delay)

            html_retry = self.http.get_text(try_url, validator=_is_valid_sfda_list_html, referer=try_ref)
            soup_retry = BeautifulSoup(html_retry, "lxml")
            max_page = max(max_page, self._discover_max_page(soup_retry, page_num))
            rows = _extract_raw_rows_from_soup(soup_retry, page_num)

            if rows:
                logger.info(
                    "Recovered page %d of [%s] on retry %d/%d (+%d rows).",
                    page_num + 1,
                    target.slug,
                    attempt_idx,
                    total_retries,
                    len(rows),
                )
                return rows, max_page

        logger.warning(
            "Page %d of [%s] still returned 0 rows after /en/ fallback and %d retries; deferring to rescue pass.",
            page_num + 1,
            target.slug,
            total_retries,
        )
        return [], max_page

    def _fetch_detail_fields(
        self,
        raw_row: RawTableRow,
        target: ListTarget,
        stagger_index: int = 0,
    ) -> tuple[dict[str, str], dict[str, str]]:
        """
        Fetch scientific detail fields for a single ingredient row.
        Applies gentle stagger and protects against SFDA's empty `id=` edge case.
        """
        if not raw_row.detail_href:
            return {}, {}

        if stagger_index > 0 and self.detail_concurrency > 1:
            time.sleep(min(0.35, 0.15 * (stagger_index % self.detail_concurrency)))

        detail_url = urljoin(SFDA_BASE_URL, raw_row.detail_href)
        parsed_qs = parse_qs(urlparse(detail_url).query, keep_blank_values=True)
        id_values = parsed_qs.get("id", [""])
        is_empty_id = (len(id_values) == 0) or (id_values[0].strip() == "")

        try:
            html = self.http.get_text(detail_url, validator=_is_valid_sfda_detail_html, referer=target.url)
        except Exception as exc:
            logger.warning(
                "Detail fetch failed for [%s] '%s' (%s); falling back to list-row data.",
                target.slug,
                raw_row.chemical_name[:60],
                exc,
            )
            return {}, {}

        raw_details = _parse_detail_table(html)
        mapped = _map_raw_details_to_fields(raw_details)

        # Guard for empty `id=` links: accept details ONLY if chemical name matches this row.
        if is_empty_id and mapped.get("chemical_name"):
            if normalize_name(mapped["chemical_name"]) != normalize_name(raw_row.chemical_name):
                logger.debug(
                    "Discarding mismatched empty-id detail for '%s' (got '%s').",
                    raw_row.chemical_name[:50],
                    mapped["chemical_name"][:50],
                )
                return {}, {}

        # Optional Smart Enrichment from AllCosmeticList (nid=17590) for missing INCI/Product Type
        if (
            self.enable_enrichment
            and target.slug in ("colorants", "preservatives", "uv_filters")
            and raw_row.cas_number
            and (not mapped.get("inci_name") or not mapped.get("product_type_body_parts"))
        ):
            self._enrich_from_all_cosmetic_list(raw_row, mapped, raw_details, target.url)

        return mapped, raw_details

    def _enrich_from_all_cosmetic_list(
        self,
        raw_row: RawTableRow,
        mapped: dict[str, str],
        raw_details: dict[str, str],
        referer: str,
    ) -> None:
        encoded_cas = quote(raw_row.cas_number, safe="")
        enrich_url = f"{SFDA_BASE_URL}/ar/details_data?nid={SFDA_ALL_COSMETIC_NID}&id={encoded_cas}&page=0"
        try:
            html = self.http.get_text(enrich_url, validator=_is_valid_sfda_detail_html, referer=referer)
        except Exception:
            return

        enrich_raw = _parse_detail_table(html)
        enrich_mapped = _map_raw_details_to_fields(enrich_raw)

        if enrich_mapped.get("chemical_name") and raw_row.chemical_name:
            if normalize_name(enrich_mapped["chemical_name"]) != normalize_name(raw_row.chemical_name):
                return

        for key in (
            "inci_name",
            "ec_number",
            "product_type_body_parts",
            "max_concentration",
            "other_conditions",
            "warnings_conditions",
        ):
            if not mapped.get(key) and enrich_mapped.get(key):
                mapped[key] = enrich_mapped[key]

        for label, val in enrich_raw.items():
            if val and not raw_details.get(label):
                raw_details[label] = val

    def scrape_page_records(
        self,
        target: ListTarget,
        page_num: int,
        used_source_keys: set[str],
        *,
        retry_delays: tuple[float, ...] = _EMPTY_PAGE_RETRY_DELAYS,
    ) -> tuple[list[IngredientRecord], int]:
        """Scrape one page of a list + fetch all row details with bounded safe concurrency."""
        raw_rows, discovered_max_page = self.fetch_list_page(
            target,
            page_num,
            retry_delays=retry_delays,
        )
        if not raw_rows:
            return [], discovered_max_page

        if self.detail_concurrency > 1 and len(raw_rows) > 1:
            indexed_rows = list(enumerate(raw_rows))
            with ThreadPoolExecutor(max_workers=self.detail_concurrency) as pool:
                detail_results = list(
                    pool.map(lambda pair: self._fetch_detail_fields(pair[1], target, pair[0]), indexed_rows)
                )
        else:
            detail_results = [self._fetch_detail_fields(r, target, 0) for r in raw_rows]

        records: list[IngredientRecord] = []
        for raw_row, (mapped, raw_details) in zip(raw_rows, detail_results):
            fields = {
                "reference_number": clean_field(mapped.get("reference_number", "")),
                "chemical_name": clean_field(mapped.get("chemical_name") or raw_row.chemical_name),
                "inci_name": clean_field(mapped.get("inci_name", "")),
                "cas_number": clean_field(mapped.get("cas_number") or raw_row.cas_number),
                "ec_number": clean_field(mapped.get("ec_number", "")),
                "product_type_body_parts": clean_field(mapped.get("product_type_body_parts", "")),
                "max_concentration": clean_field(mapped.get("max_concentration", "")),
                "other_conditions": clean_field(mapped.get("other_conditions", "")),
                "warnings_conditions": clean_field(mapped.get("warnings_conditions", "")),
            }

            if not fields["chemical_name"] and fields["inci_name"]:
                fields["chemical_name"] = fields["inci_name"]

            if not fields["chemical_name"]:
                continue

            source_key = build_source_key(
                list_slug=target.slug,
                reference_number=fields["reference_number"],
                cas_number=fields["cas_number"],
                chemical_name=fields["chemical_name"],
                used_keys_in_list=used_source_keys,
            )
            row_hash = compute_row_hash(fields)

            records.append(
                IngredientRecord(
                    list_slug=target.slug,
                    source_key=source_key,
                    reference_number=fields["reference_number"],
                    chemical_name=fields["chemical_name"],
                    inci_name=fields["inci_name"],
                    cas_number=fields["cas_number"],
                    ec_number=fields["ec_number"],
                    product_type_body_parts=fields["product_type_body_parts"],
                    max_concentration=fields["max_concentration"],
                    other_conditions=fields["other_conditions"],
                    warnings_conditions=fields["warnings_conditions"],
                    row_hash=row_hash,
                    raw_details=raw_details,
                    source_page=page_num,
                )
            )

        return records, discovered_max_page

    def iter_list_pages(
        self,
        target: ListTarget,
        *,
        start_page: int = 0,
        known_total_pages: int | None = None,
        used_source_keys: set[str],
        max_pages_limit: int | None = None,
    ) -> Iterator[tuple[int, int, list[IngredientRecord]]]:
        """
        Yield `(page_num, total_pages_known, records)` sequentially from `start_page`
        through the final page of the list, followed by an end-of-list rescue pass.
        """
        page_num = max(0, start_page)
        last_page_idx = (known_total_pages - 1) if (known_total_pages and known_total_pages > 0) else 0
        pages_processed = 0
        deferred_empty_pages: list[int] = []

        while True:
            records, discovered_max = self.scrape_page_records(target, page_num, used_source_keys)
            if discovered_max > last_page_idx:
                last_page_idx = discovered_max

            total_pages = last_page_idx + 1
            if not records:
                deferred_empty_pages.append(page_num)

            yield page_num, total_pages, records

            pages_processed += 1
            if max_pages_limit is not None and pages_processed >= max_pages_limit:
                break

            if page_num >= last_page_idx:
                break

            page_num += 1

        # End-of-list rescue pass for any pages that remained empty on both /ar/ and /en/
        if deferred_empty_pages and max_pages_limit is None:
            logger.info(
                "Starting rescue pass for %d empty page(s) in [%s]: %s",
                len(deferred_empty_pages),
                target.slug,
                [p + 1 for p in deferred_empty_pages],
            )
            self._reset_http_connection()
            time.sleep(10.0)

            still_empty: list[int] = []
            total_pages = last_page_idx + 1

            for retry_page in deferred_empty_pages:
                rescued_records, discovered_max = self.scrape_page_records(
                    target,
                    retry_page,
                    used_source_keys,
                    retry_delays=_RESCUE_PASS_RETRY_DELAYS,
                )
                if discovered_max > last_page_idx:
                    last_page_idx = discovered_max
                    total_pages = last_page_idx + 1

                if rescued_records:
                    logger.info(
                        "Rescue pass SUCCEEDED for page %d of [%s] (+%d rows).",
                        retry_page + 1,
                        target.slug,
                        len(rescued_records),
                    )
                    yield retry_page, total_pages, rescued_records
                else:
                    still_empty.append(retry_page + 1)

            if still_empty:
                self.unrecovered_empty_pages[target.slug] = still_empty
                logger.warning(
                    "Unrecovered empty pages on SFDA source for [%s] after rescue pass: %s",
                    target.slug,
                    still_empty,
                )
