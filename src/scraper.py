"""
SFDA Cosmetic Lists HTML Scraper & Detail Extractor.
Built directly against the verified Drupal HTML & `/ar/details_data` structure from Stage 1.
Includes strict non-empty table validation (fixes transient +0 rows pages) and
gentle concurrency throttling to minimize Connection Reset (Errno 104) errors.
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
from .http_client import ResilientHttpClient, SoftBlockError
from .models import IngredientRecord

logger = logging.getLogger(__name__)

_PAGE_PARAM_RE = re.compile(r"[?&]page=(\d+)")

# Normalized lowercase label -> canonical field key
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
    """
    Confirm the response is a genuine SFDA Drupal list page AND actually contains
    at least one ingredient row (`<td>` cells). Rejects transient HTTP-200 empty
    table responses so ResilientHttpClient retries automatically with backoff.
    """
    if len(html) < 5000:
        return False
    if "<table" not in html or "<td" not in html:
        return False

    soup = BeautifulSoup(html, "lxml")
    rows = _extract_raw_rows_from_soup(soup, 0)
    return len(rows) > 0


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

    def fetch_list_page(self, target: ListTarget, page_num: int) -> tuple[list[RawTableRow], int]:
        """
        Fetch a single paginated list page (`?page=N`) and return:
        `(rows_on_page, discovered_max_page_index)`.
        Guarantees non-empty rows via `_is_valid_sfda_list_html` + explicit guard.
        """
        page_url = f"{target.url}?page={page_num}" if page_num > 0 else target.url
        html = self.http.get_text(page_url, validator=_is_valid_sfda_list_html, referer=target.url)
        soup = BeautifulSoup(html, "lxml")

        max_page = page_num
        for anchor in soup.select("ul.pagination a[href], nav a[href]"):
            href = anchor.get("href") or ""
            match = _PAGE_PARAM_RE.search(href)
            if match:
                idx = int(match.group(1))
                if idx > max_page:
                    max_page = idx

        rows = _extract_raw_rows_from_soup(soup, page_num)
        if not rows:
            raise SoftBlockError(f"Parsed 0 ingredient rows from {page_url} despite valid HTML shell.")

        return rows, max_page

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
