#!/usr/bin/env python3
"""
Entrypoint for SFDA Food & Cosmetics Circulars/Decisions Sync Worker.
Supports `auto` (initial full baseline -> subsequent fast incremental scans),
`full`, and `incremental` modes, with local JSON backup and WP email digest trigger.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .circulars_scraper import DEFAULT_SECTOR_IDS, SfdaCircularsScraper
from .circulars_uploader import CircularsUploader
from .http_client import ResilientHttpClient

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SFDA Food & Cosmetics Circulars/Decisions Sync Worker -> WordPress Mirror"
    )
    parser.add_argument(
        "--mode",
        choices=("auto", "full", "incremental"),
        default="auto",
        help="auto: full baseline on empty DB, incremental afterward.",
    )
    parser.add_argument(
        "--sectors",
        default="food,cosmetics",
        help="Comma-separated sectors (food,cosmetics).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape SFDA circulars and write local JSON export without calling WordPress.",
    )
    parser.add_argument(
        "--force-baseline",
        action="store_true",
        help="Mark newly inserted circulars as baseline (suppresses unread/email alerts).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional page cap per sector for smoke testing.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def run_circulars_sync(args: argparse.Namespace) -> int:
    load_dotenv()
    configure_logging(verbose=args.verbose)
    log = logging.getLogger("circulars_sync")

    started = time.monotonic()
    state_dir = Path(os.getenv("SFDA_STATE_DIR", "state")).resolve()
    delay_sec = max(0.2, float(os.getenv("SFDA_REQUEST_DELAY_SEC", "0.50")))
    concurrency = max(1, min(2, int(os.getenv("SFDA_DETAIL_CONCURRENCY", "2"))))
    timeout_sec = max(15.0, float(os.getenv("SFDA_HTTP_TIMEOUT_SEC", "45")))
    max_retries = max(1, min(10, int(os.getenv("SFDA_MAX_RETRIES", "5"))))
    batch_size = max(10, min(100, int(os.getenv("SFDA_CIRCULARS_BATCH_SIZE", "50"))))

    raw_sectors = [s.strip().lower() for s in (args.sectors or "food,cosmetics").split(",") if s.strip()]
    sectors = tuple(s for s in ("food", "cosmetics") if s in raw_sectors)
    if not sectors:
        raise ValueError(f"Invalid sectors '{args.sectors}'. Allowed: {list(DEFAULT_SECTOR_IDS.keys())}")

    wp_base = (os.getenv("WP_CIRCULARS_BASE_URL") or os.getenv("WP_SYNC_BASE_URL") or "").strip()
    wp_key = (os.getenv("WP_SFDA_API_KEY") or "").strip()
    wp_secret = (os.getenv("WP_SFDA_HMAC_SECRET") or "").strip()

    if not args.dry_run and (not wp_base or not wp_key or not wp_secret):
        raise ValueError(
            "Missing WP_SYNC_BASE_URL / WP_SFDA_API_KEY / WP_SFDA_HMAC_SECRET environment variables."
        )

    session_id = f"circ-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:10]}"
    summary: dict[str, Any] = {
        "session_id": session_id,
        "requested_mode": args.mode,
        "sectors": list(sectors),
        "dry_run": bool(args.dry_run),
        "started_at_utc": utc_now_iso(),
        "status": "running",
    }

    uploader: CircularsUploader | None = None
    try:
        known_nids: set[int] = set()
        has_baseline = False

        if not args.dry_run:
            uploader = CircularsUploader(
                wp_sync_base_url=wp_base,
                api_key=wp_key,
                hmac_secret=wp_secret,
                timeout_sec=timeout_sec,
                max_retries=max_retries,
            )
            wp_status = uploader.check_status()
            wp_stats = wp_status.get("stats") or {}
            has_baseline = bool(wp_stats.get("has_baseline"))
            known_nids = {int(n) for n in (wp_status.get("known_nids") or []) if int(n) > 0}
            log.info(
                "Connected to WP Circulars API (%s) | Existing: %d (food=%s, cosmetics=%s, has_baseline=%s)",
                uploader.base_url,
                len(known_nids),
                wp_stats.get("food_count", 0),
                wp_stats.get("cosmetics_count", 0),
                has_baseline,
            )

        # Resolve effective scrape mode & baseline alert behavior
        if args.mode == "auto":
            effective_mode = "incremental" if has_baseline else "full"
        else:
            effective_mode = args.mode

        is_baseline = bool(args.force_baseline) or (not has_baseline)
        summary["effective_mode"] = effective_mode
        summary["is_baseline"] = is_baseline

        log.info(
            "Starting Circulars Sync %s | Mode: %s | Baseline: %s | Sectors: %s",
            session_id,
            effective_mode,
            is_baseline,
            ",".join(sectors),
        )

        with ResilientHttpClient(
            timeout_sec=timeout_sec,
            max_retries=max_retries,
            min_delay_sec=delay_sec,
            max_connections=concurrency + 4,
        ) as http_client:
            scraper = SfdaCircularsScraper(http_client, detail_concurrency=concurrency)
            records, scrape_stats = scraper.scrape_sectors(
                sectors=sectors,
                mode=effective_mode,
                known_nids=known_nids,
                max_pages_per_sector=args.max_pages,
            )

        summary["scrape_stats"] = scrape_stats

        export_path = state_dir / "sfda_circulars_export.json"
        _atomic_write_json(
            export_path,
            {
                "schema_version": "1.0",
                "generated_at_utc": utc_now_iso(),
                "session_id": session_id,
                "mode": effective_mode,
                "is_baseline": is_baseline,
                "total_items": len(records),
                "items": records,
            },
        )
        log.info("Circulars JSON backup written to: %s (%d items)", export_path, len(records))

        if not args.dry_run and uploader is not None:
            totals = {"inserted": 0, "updated": 0, "unchanged": 0, "skipped": 0, "new_alerts": 0}
            for idx in range(0, len(records), batch_size):
                chunk = records[idx : idx + batch_size]
                res = uploader.push_batch(session_id, chunk, is_baseline=is_baseline)
                for k in totals:
                    totals[k] += int(res.get(k, 0))
                log.info(
                    "  -> Batch %d-%d: inserted=%s, updated=%s, unchanged=%s, new_alerts=%s",
                    idx + 1,
                    idx + len(chunk),
                    res.get("inserted", 0),
                    res.get("updated", 0),
                    res.get("unchanged", 0),
                    res.get("new_alerts", 0),
                )

            finish_res = uploader.finish_sync(session_id, is_baseline=is_baseline)
            summary["ingest_totals"] = totals
            summary["finish_result"] = finish_res
            summary["status"] = "completed"

            email_info = finish_res.get("email_notification") or {}
            log.info(
                "CIRCULARS SYNC FINISHED | Inserted=%d, Updated=%d, NewAlerts=%d | Email: sent=%s (%s)",
                totals["inserted"],
                totals["updated"],
                finish_res.get("new_circulars_count", 0),
                email_info.get("sent"),
                email_info.get("reason"),
            )
        else:
            summary["status"] = "dry_run_completed"

        summary["elapsed_sec"] = round(time.monotonic() - started, 2)
        summary["finished_at_utc"] = utc_now_iso()
        _atomic_write_json(state_dir / "circulars_summary.json", summary)
        log.info("Done cleanly in %.1fs.", summary["elapsed_sec"])
        return 0

    except Exception as exc:
        log.exception("Circulars sync failed: %s", exc)
        summary["status"] = "failed"
        summary["error"] = str(exc)
        summary["elapsed_sec"] = round(time.monotonic() - started, 2)
        summary["finished_at_utc"] = utc_now_iso()
        _atomic_write_json(state_dir / "circulars_summary.json", summary)
        return 1
    finally:
        if uploader is not None:
            uploader.close()


if __name__ == "__main__":
    raise SystemExit(run_circulars_sync(build_arg_parser().parse_args()))