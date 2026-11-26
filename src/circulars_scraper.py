"""
SFDA Food & Cosmetics Circulars/Decisions Scraper.
Targets `https://www.sfda.gov.sa/ar/circulars` strictly for Food (4) and Cosmetics (7),
sorted newest-first (`field_date_value_DESC`), with automatic cross-sector merging
and bounded-concurrency detail page extraction.
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag

from .config import SFDA_BASE_URL
from .hasher import clean_field
from .http_client import ResilientHttpClient

logger = logging.getLogger(__name__)

CIRCULARS_BASE_URL = f"{SFDA_BASE_URL}/ar/circulars"

# Verified Drupal taxonomy term IDs from Stage 1 discovery
DEFAULT_SECTOR_IDS: dict[str, str] = {
    "food": "4",
    "cosmetics": "7",
}

_PAGE_PARAM_RE = re.compile(r"[?&]page=(\d+)")
_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_NODE_CLASS_RE = re.compile(r"\bpage-node-(\d+)\b")
_URL_NID_RE = re.compile(r"/circulars/(\d+)(?:$|[/?#])")


@dataclass
class CircularCard:
    source_nid: int
    title: str
    publish_date: str
    sectors: set[str] = field(default_factory=set)
    detail_url: str = ""
    pdf_url: str = ""


def _normalize_url(raw_href: str) -> str:
    href = (raw_href or "").strip()
    if not href:
        return ""
    abs_url = urljoin(SFDA_BASE_URL, href)
    parts = urlsplit(abs_url)
    # Encode raw non-ASCII / space characters without double-encoding existing %XX sequences
    safe_path = quote(parts.path, safe="/%:@!$&'()*+,;=-._~")
    return urlunsplit((parts.scheme, parts.netloc, safe_path, parts.query, parts.fragment))


def _is_valid_circulars_list_html(html: str) -> bool:
    if len(html) < 4000:
        return False
    low = html.lower()
    return ("node--type-circulars" in html) or ("field_type_target_id" in html) or ("sfda" in low and "<form" in low)


def _is_valid_circular_detail_html(html: str) -> bool:
    if len(html) < 2500:
        return False
    return ("bg-light" in html) or ("field--name-body" in html) or ("page-node-" in html)


def _map_sector_label(text: str) -> str | None:
    clean = clean_field(text).lower()
    if clean in ("غذاء", "الغذاء", "food"):
        return "food"
    if clean in ("تجميل", "التجميل", "منتجات التجميل", "cosmetics", "cosmetic"):
        return "cosmetics"
    return None


def _parse_cards_from_soup(soup: BeautifulSoup, queried_sector: str) -> list[CircularCard]:
    cards: list[CircularCard] = []
    for node in soup.select(".node--type-circulars"):
        raw_nid = (node.get("data-history-node-id") or "").strip()
        nid = int(raw_nid) if raw_nid.isdigit() else 0

        title_el = node.select_one("p.head-subtitle") or node.select_one("h2, h3")
        title = clean_field(title_el.get_text(" ", strip=True)) if title_el else ""

        publish_date = ""
        date_el = node.select_one("div.mb-2 span.mb-0")
        if date_el:
            m_date = _DATE_RE.search(date_el.get_text(" ", strip=True))
            if m_date:
                publish_date = m_date.group(1)
        if not publish_date:
            m_date = _DATE_RE.search(node.get_text(" ", strip=True))
            if m_date:
                publish_date = m_date.group(1)

        sectors: set[str] = {queried_sector}
        for tag_el in node.select(".tags .tag"):
            mapped = _map_sector_label(tag_el.get_text(" ", strip=True))
            if mapped:
                sectors.add(mapped)

        detail_url = ""
        pdf_url = ""
        for anchor in node.select("a[href]"):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            cls_str = " ".join(anchor.get("class") or [])
            link_txt = clean_field(anchor.get_text(" ", strip=True))
            low_href = href.lower()

            if (".pdf" in low_href) or ("/sites/default/files/" in low_href) or ("تحميل" in link_txt):
                if not pdf_url:
                    pdf_url = _normalize_url(href)
            elif ("btn-outline-primary" in cls_str) or ("المزيد" in link_txt) or ("/circulars/" in low_href):
                if not detail_url:
                    detail_url = _normalize_url(href)

        if nid <= 0 and detail_url:
            m_url_nid = _URL_NID_RE.search(detail_url)
            if m_url_nid:
                nid = int(m_url_nid.group(1))

        if nid <= 0 or not title:
            continue

        cards.append(
            CircularCard(
                source_nid=nid,
                title=title,
                publish_date=publish_date,
                sectors=sectors,
                detail_url=detail_url,
                pdf_url=pdf_url,
            )
        )

    return cards


def _discover_max_page(soup: BeautifulSoup, current_page: int) -> int:
    max_page = current_page
    for anchor in soup.select("nav a[href*='page='], ul.pagination a[href*='page='], .pager a[href*='page=']"):
        href = anchor.get("href") or ""
        m = _PAGE_PARAM_RE.search(href)
        if m:
            idx = int(m.group(1))
            if idx > max_page:
                max_page = idx
    return max_page


def _discover_sector_ids_from_soup(soup: BeautifulSoup) -> dict[str, str]:
    """Read live option IDs from `<select name='field_type_target_id'>` if available."""
    resolved = dict(DEFAULT_SECTOR_IDS)
    sel = soup.select_one("select[name='field_type_target_id']")
    if not sel:
        return resolved
    for opt in sel.select("option"):
        val = (opt.get("value") or "").strip()
        txt = clean_field(opt.get_text(" ", strip=True))
        if not val or val.lower() == "all":
            continue
        mapped = _map_sector_label(txt)
        if mapped:
            resolved[mapped] = val
    return resolved


def _parse_detail_html(html: str, card: CircularCard) -> dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")

    container: Tag = (
        soup.select_one("main .bg-light")
        or soup.select_one(".bg-light.rounded-3")
        or soup.select_one("main")
        or soup
    )

    # Confirm NID if needed
    nid = card.source_nid
    if nid <= 0:
        body_tag = soup.find("body")
        body_cls = " ".join(body_tag.get("class") or []) if body_tag else ""
        m_nid = _NODE_CLASS_RE.search(body_cls)
        if m_nid:
            nid = int(m_nid.group(1))

    title = card.title
    h2 = container.select_one("h2.fs-3") or container.select_one("h1, h2")
    if h2:
        h2_txt = clean_field(h2.get_text(" ", strip=True))
        if h2_txt:
            title = h2_txt

    publish_date = card.publish_date
    if not publish_date:
        date_el = container.select_one(".text-primary span.mb-0")
        if date_el:
            m_d = _DATE_RE.search(date_el.get_text(" ", strip=True))
            if m_d:
                publish_date = m_d.group(1)

    sectors = set(card.sectors)
    doc_type = "circular"
    doc_type_ar = "تعميم"
    audience_parts: list[str] = []

    # Parse top badges (excluding country badges inside the "بلد المنشأ" block)
    for badge in container.select("span.badge"):
        parent_mb3 = badge.find_parent("div", class_="mb-3")
        if parent_mb3 is not None and parent_mb3.find("strong") is not None:
            continue
        btxt = clean_field(badge.get_text(" ", strip=True))
        if not btxt:
            continue
        mapped_sec = _map_sector_label(btxt)
        if mapped_sec:
            sectors.add(mapped_sec)
        elif btxt == "قرار":
            doc_type = "decision"
            doc_type_ar = "قرار"
        elif btxt == "تعميم":
            doc_type = "circular"
            doc_type_ar = "تعميم"
        elif btxt in ("مستثمر", "فرد", "ممارس صحي"):
            if btxt not in audience_parts:
                audience_parts.append(btxt)

    circular_status = ""
    decision_status = ""
    target_client = ""
    purpose = ""
    countries: list[str] = []

    for block in container.select("div.mb-3"):
        strong = block.find("strong")
        if strong is None:
            continue
        label = clean_field(strong.get_text(" ", strip=True)).rstrip(":")

        if "بلد المنشأ" in label or "الدول" in label:
            for c_badge in block.select("span.badge"):
                cname = clean_field(c_badge.get_text(" ", strip=True))
                if cname and cname not in countries:
                    countries.append(cname)
            continue

        span_val = block.find("span")
        val = clean_field(span_val.get_text(" ", strip=True)) if span_val else ""
        if not val:
            # Fallback to text minus label
            full_blk = clean_field(block.get_text(" ", strip=True))
            val = clean_field(full_blk.replace(strong.get_text(" ", strip=True), "", 1))

        if "حالة التعميم" in label:
            circular_status = val
        elif "حالة القرار" in label:
            decision_status = val
        elif "العميل المستهدف" in label:
            target_client = val
        elif "الغرض من التعميم" in label or "الغرض" in label:
            purpose = val

    body_html = ""
    body_text = ""
    body_el = container.select_one(".field--name-body")
    if body_el:
        for script_or_style in body_el.select("script, style"):
            script_or_style.decompose()
        body_html = body_el.decode_contents().strip()
        body_text = clean_field(body_el.get_text(" ", strip=True))

    pdf_url = card.pdf_url
    if not pdf_url:
        for anchor in container.select("a[href]"):
            href = (anchor.get("href") or "").strip()
            low_h = href.lower()
            if ".pdf" in low_h or "/sites/default/files/" in low_h:
                pdf_url = _normalize_url(href)
                break

    return {
        "source_nid": nid,
        "title": title,
        "publish_date": publish_date,
        "sectors": sorted(sectors),
        "doc_type": doc_type,
        "doc_type_ar": doc_type_ar,
        "audience_ar": "، ".join(audience_parts),
        "circular_status": circular_status,
        "decision_status": decision_status,
        "target_client": target_client,
        "purpose": purpose,
        "countries": countries,
        "body_html": body_html,
        "body_text": body_text,
        "detail_url": card.detail_url,
        "pdf_url": pdf_url,
    }


class SfdaCircularsScraper:
    MAX_SAFE_CONCURRENCY = 2

    def __init__(
        self,
        http_client: ResilientHttpClient,
        *,
        detail_concurrency: int = 2,
    ) -> None:
        self.http = http_client
        self.detail_concurrency = max(1, min(self.MAX_SAFE_CONCURRENCY, detail_concurrency))

    def _build_list_url(self, sector_id: str, page_num: int) -> str:
        return (
            f"{CIRCULARS_BASE_URL}"
            f"?keywords="
            f"&field_type_circulars_target_id=All"
            f"&field_category_target_id=All"
            f"&field_type_target_id={sector_id}"
            f"&sort_bef_combine=field_date_value_DESC"
            f"&page={page_num}"
        )

    def _fetch_list_page(self, sector_slug: str, sector_id: str, page_num: int) -> tuple[list[CircularCard], int, dict[str, str]]:
        url = self._build_list_url(sector_id, page_num)
        html = self.http.get_text(url, validator=_is_valid_circulars_list_html, referer=CIRCULARS_BASE_URL)
        soup = BeautifulSoup(html, "lxml")
        discovered_ids = _discover_sector_ids_from_soup(soup) if page_num == 0 else {}
        max_page = _discover_max_page(soup, page_num)
        cards = _parse_cards_from_soup(soup, sector_slug)

        # Single clean retry if an interior page unexpectedly returned 0 cards
        if not cards and page_num > 0:
            logger.warning("Circulars [%s] page %d returned 0 cards; retrying with fresh session...", sector_slug, page_num + 1)
            self.http.reset_session()
            time.sleep(5.0)
            html_retry = self.http.get_text(
                f"{url}&_cb={int(time.time())}",
                validator=_is_valid_circulars_list_html,
                referer=CIRCULARS_BASE_URL,
            )
            soup_retry = BeautifulSoup(html_retry, "lxml")
            max_page = max(max_page, _discover_max_page(soup_retry, page_num))
            cards = _parse_cards_from_soup(soup_retry, sector_slug)

        return cards, max_page, discovered_ids

    def _fetch_detail_record(self, card: CircularCard, stagger_idx: int = 0) -> dict[str, Any]:
        if stagger_idx > 0 and self.detail_concurrency > 1:
            time.sleep(min(0.35, 0.15 * (stagger_idx % self.detail_concurrency)))

        if not card.detail_url:
            return {
                "source_nid": card.source_nid,
                "title": card.title,
                "publish_date": card.publish_date,
                "sectors": sorted(card.sectors),
                "doc_type": "circular",
                "doc_type_ar": "تعميم",
                "audience_ar": "",
                "circular_status": "",
                "decision_status": "",
                "target_client": "",
                "purpose": "",
                "countries": [],
                "body_html": "",
                "body_text": "",
                "detail_url": "",
                "pdf_url": card.pdf_url,
            }

        try:
            html = self.http.get_text(
                card.detail_url,
                validator=_is_valid_circular_detail_html,
                referer=CIRCULARS_BASE_URL,
            )
            return _parse_detail_html(html, card)
        except Exception as exc:
            logger.warning(
                "Detail fetch failed for circular NID=%d ('%s'): %s — saving card-level metadata.",
                card.source_nid,
                card.title[:60],
                exc,
            )
            return {
                "source_nid": card.source_nid,
                "title": card.title,
                "publish_date": card.publish_date,
                "sectors": sorted(card.sectors),
                "doc_type": "circular",
                "doc_type_ar": "تعميم",
                "audience_ar": "",
                "circular_status": "",
                "decision_status": "",
                "target_client": "",
                "purpose": "",
                "countries": [],
                "body_html": "",
                "body_text": "",
                "detail_url": card.detail_url,
                "pdf_url": card.pdf_url,
            }

    def scrape_sectors(
        self,
        sectors: tuple[str, ...] = ("food", "cosmetics"),
        *,
        mode: str = "full",
        known_nids: set[int] | None = None,
        max_pages_per_sector: int | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """
        Scrape Food & Cosmetics circulars, merge cross-sector duplicates by `source_nid`,
        and enrich with full detail pages (skipping detail fetches for already-known NIDs
        during `incremental` mode).
        """
        known = known_nids or set()
        is_incremental = (mode == "incremental") and bool(known)

        sector_ids = dict(DEFAULT_SECTOR_IDS)
        cards_by_nid: dict[int, CircularCard] = {}
        stats: dict[str, Any] = {"sectors": {}, "mode_used": "incremental" if is_incremental else "full"}

        for sector_slug in sectors:
            if sector_slug not in sector_ids:
                logger.warning("Skipping unsupported sector '%s' (allowed: food, cosmetics).", sector_slug)
                continue

            sec_id = sector_ids[sector_slug]
            page_num = 0
            last_page_idx = 0
            pages_scanned = 0
            cards_seen = 0
            new_in_sector = 0
            consecutive_all_known_pages = 0

            logger.info(
                "Scanning SFDA circulars for [%s] (field_type_target_id=%s, mode=%s)...",
                sector_slug,
                sec_id,
                stats["mode_used"],
            )

            while True:
                page_cards, discovered_max, discovered_ids = self._fetch_list_page(sector_slug, sec_id, page_num)
                if page_num == 0 and discovered_ids:
                    sector_ids.update(discovered_ids)
                if discovered_max > last_page_idx:
                    last_page_idx = discovered_max

                pages_scanned += 1
                if not page_cards:
                    logger.info("  [%s] Page %d returned 0 cards; stopping sector scan.", sector_slug, page_num + 1)
                    break

                page_new_count = 0
                for card in page_cards:
                    cards_seen += 1
                    is_new = card.source_nid not in known
                    if is_new:
                        page_new_count += 1
                        new_in_sector += 1

                    # In incremental mode, keep new cards + any card seen on page 0 (for status updates)
                    if is_incremental and not is_new and page_num >= 1:
                        continue

                    if card.source_nid in cards_by_nid:
                        existing_card = cards_by_nid[card.source_nid]
                        existing_card.sectors.update(card.sectors)
                        if not existing_card.pdf_url and card.pdf_url:
                            existing_card.pdf_url = card.pdf_url
                        if not existing_card.detail_url and card.detail_url:
                            existing_card.detail_url = card.detail_url
                    else:
                        cards_by_nid[card.source_nid] = card

                logger.info(
                    "  [%s] Page %d/%d scanned (%d cards, %d new)",
                    sector_slug,
                    page_num + 1,
                    last_page_idx + 1,
                    len(page_cards),
                    page_new_count,
                )

                if is_incremental:
                    if page_new_count == 0:
                        consecutive_all_known_pages += 1
                    else:
                        consecutive_all_known_pages = 0
                    # Stop cleanly after 2 consecutive pages with zero new circulars
                    if consecutive_all_known_pages >= 2:
                        logger.info(
                            "  [%s] Incremental stop reached after %d consecutive known pages.",
                            sector_slug,
                            consecutive_all_known_pages,
                        )
                        break

                if max_pages_per_sector is not None and pages_scanned >= max_pages_per_sector:
                    break
                if page_num >= last_page_idx:
                    break
                page_num += 1

            stats["sectors"][sector_slug] = {
                "pages_scanned": pages_scanned,
                "total_pages_discovered": last_page_idx + 1,
                "cards_seen": cards_seen,
                "new_nids_found": new_in_sector,
            }

        unique_cards = list(cards_by_nid.values())
        logger.info(
            "Fetching detail pages for %d unique Food/Cosmetics circular(s) (concurrency=%d)...",
            len(unique_cards),
            self.detail_concurrency,
        )

        records: list[dict[str, Any]] = []
        if unique_cards:
            if self.detail_concurrency > 1 and len(unique_cards) > 1:
                indexed = list(enumerate(unique_cards))
                with ThreadPoolExecutor(max_workers=self.detail_concurrency) as pool:
                    records = list(
                        pool.map(lambda pair: self._fetch_detail_record(pair[1], pair[0]), indexed)
                    )
            else:
                records = [self._fetch_detail_record(c, 0) for c in unique_cards]

        # Sort newest first for clean export & ingestion
        records.sort(key=lambda r: (r.get("publish_date") or "", int(r.get("source_nid") or 0)), reverse=True)
        stats["total_records_prepared"] = len(records)
        return records, stats