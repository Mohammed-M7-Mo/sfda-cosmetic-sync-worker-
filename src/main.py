#!/usr/bin/env python3
"""
Entrypoint for the Resilient SFDA Cosmetic Mirror Sync Worker.
Orchestrates checkpoint loading, polite paginated scraping, batch staging,
emergency JSON export generation, and atomic WordPress commit.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any

from .checkpoint import CheckpointManager, utc_now_iso
from .config import SFDA_TARGETS_BY_SLUG, WorkerConfig
from .http_client import ResilientHttpClient
from .models import IngredientRecord
from .scraper import SfdaScraper
from .wp_uploader import WordPressUploader

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


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
        description="Resilient SFDA Cosmetic Lists Sync Worker -> WordPress Mirror"
    )
    parser.add_argument(
        "--lists",
        default=None,
        help="Comma-separated list slugs (e.g., uv_filters,preservatives) or 'all'.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape SFDA and generate local emergency JSON export without calling WordPress.",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Ignore any unfinished checkpoint and start a fresh sync session.",
    )
    parser.add_argument(
        "--force-commit",
        action="store_true",
        help="Pass force_commit=true to WordPress (only if an intentional major drop occurred).",
    )
    parser.add_argument(
        "--max-pages-per-list",
        type=int,
        default=None,
        help="Optional page cap per list for quick smoke testing (disables WP commit unless --dry-run).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging.",
    )
    return parser


def flush_batch(
    uploader: WordPressUploader | None,
    session_uuid: str,
    list_slug: str,
    buffer: list[IngredientRecord],
    batch_size: int,
) -> None:
    if not buffer:
        return
    if uploader is None:
        buffer.clear()
        return

    for idx in range(0, len(buffer), batch_size):
        chunk = buffer[idx : idx + batch_size]
        res = uploader.push_batch(session_uuid, list_slug, chunk)
        logging.getLogger("sync").info(
            "  -> Staged batch for [%s]: %d items (session total staged: %s)",
            list_slug,
            res.get("staged", len(chunk)),
            res.get("total_staged", "?"),
        )
    buffer.clear()


def run_sync(args: argparse.Namespace) -> int:
    configure_logging(verbose=args.verbose)
    log = logging.getLogger("sync")

    started_monotonic = time.monotonic()
    config = WorkerConfig.from_env(
        lists_scope_override=args.lists,
        dry_run=args.dry_run,
        force_commit=args.force_commit,
        max_pages_per_list=args.max_pages_per_list,
    )

    if config.max_pages_per_list is not None and not config.dry_run:
        log.warning(
            "--max-pages-per-list is set without --dry-run; commit will be SKIPPED to protect live WordPress data."
        )

    cp_mgr = CheckpointManager(config.state_dir, ttl_hours=config.checkpoint_ttl_hours)
    state, resumed = cp_mgr.load_or_create(
        config.lists_scope,
        config.scope_label,
        force_reset=args.reset_checkpoint,
    )

    log.info(
        "Sync session %s (%s) | Scope: %s | Dry-Run: %s | Enrichment: %s",
        state.session_uuid,
        "RESUMED from checkpoint" if resumed else "NEW",
        config.scope_label,
        config.dry_run,
        config.enable_enrichment,
    )

    summary: dict[str, Any] = {
        "session_uuid": state.session_uuid,
        "resumed": resumed,
        "scope": config.scope_label,
        "dry_run": config.dry_run,
        "started_at_utc": utc_now_iso(),
        "status": "running",
        "lists": {},
    }

    uploader: WordPressUploader | None = None
    try:
        if not config.dry_run:
            uploader = WordPressUploader(
                base_url=config.wp_sync_base_url,
                api_key=config.wp_api_key,
                hmac_secret=config.wp_hmac_secret,
                timeout_sec=config.http_timeout_sec,
                max_retries=config.max_retries,
            )
            wp_status = uploader.check_status()
            log.info(
                "Connected to WordPress Mirror v%s (live items: %s)",
                wp_status.get("plugin_version", "?"),
                wp_status.get("total_live_items", "?"),
            )
            uploader.start_session(
                session_uuid=state.session_uuid,
                trigger_source=config.trigger_source,
                lists_scope=config.scope_label,
            )
            state.session_started_on_wp = True
            cp_mgr.save(state)

        with ResilientHttpClient(
            timeout_sec=config.http_timeout_sec,
            max_retries=config.max_retries,
            min_delay_sec=config.request_delay_sec,
            max_connections=config.detail_concurrency + 4,
        ) as http_client:
            scraper = SfdaScraper(
                http_client,
                detail_concurrency=config.detail_concurrency,
                enable_enrichment=config.enable_enrichment,
            )

            for slug in config.lists_scope:
                target = SFDA_TARGETS_BY_SLUG[slug]
                prog = state.lists[slug]

                if prog.completed:
                    log.info("Skipping [%s] — already completed in checkpoint (%d items).", slug, prog.items_scraped)
                    summary["lists"][slug] = {
                        "items_scraped": prog.items_scraped,
                        "total_pages": prog.total_pages,
                        "completed": True,
                    }
                    continue

                start_page = prog.last_completed_page + 1
                used_keys = set(prog.used_source_keys)
                batch_buffer: list[IngredientRecord] = []

                log.info(
                    "Scraping [%s] (%s) starting at page %d...",
                    slug,
                    target.url,
                    start_page,
                )

                for page_num, total_pages, page_records in scraper.iter_list_pages(
                    target,
                    start_page=start_page,
                    known_total_pages=prog.total_pages,
                    used_source_keys=used_keys,
                    max_pages_limit=config.max_pages_per_list,
                ):
                    prog.total_pages = total_pages
                    cp_mgr.append_list_records(slug, page_records)
                    batch_buffer.extend(page_records)

                    # Flush to WP staging when buffer reaches batch_size or on the final page.
                    is_last_page = (page_num + 1) >= total_pages
                    if len(batch_buffer) >= config.batch_size or is_last_page or config.max_pages_per_list is not None:
                        flush_batch(uploader, state.session_uuid, slug, batch_buffer, config.batch_size)

                    prog.last_completed_page = page_num
                    prog.items_scraped += len(page_records)
                    prog.used_source_keys = list(used_keys)
                    cp_mgr.save(state)

                    log.info(
                        "  [%s] Page %d/%d done (+%d rows, cumulative: %d)",
                        slug,
                        page_num + 1,
                        total_pages,
                        len(page_records),
                        prog.items_scraped,
                    )

                # Flush any remaining records and mark list completed only if full pagination finished.
                flush_batch(uploader, state.session_uuid, slug, batch_buffer, config.batch_size)
                if config.max_pages_per_list is None:
                    prog.completed = True
                    cp_mgr.save(state)

                summary["lists"][slug] = {
                    "items_scraped": prog.items_scraped,
                    "total_pages": prog.total_pages,
                    "completed": prog.completed,
                }

        # Always generate the emergency JSON backup file.
        export_file = cp_mgr.build_emergency_export(state)
        log.info("Emergency JSON backup written to: %s", export_file)

        all_completed = all(state.lists[s].completed for s in config.lists_scope)

        if not config.dry_run and uploader is not None:
            if all_completed and config.max_pages_per_list is None:
                expected_counts = {s: state.lists[s].items_scraped for s in config.lists_scope}
                commit_res = uploader.commit_session(
                    session_uuid=state.session_uuid,
                    completed_lists=list(config.lists_scope),
                    expected_counts=expected_counts,
                    force_commit=config.force_commit,
                )
                log.info("ATOMIC COMMIT SUCCEEDED: %s", commit_res.get("stats"))
                summary["status"] = "committed"
                summary["commit_result"] = commit_res
                cp_mgr.clear_checkpoint_only()
            else:
                log.warning("Run stopped before full completion; staging kept intact without committing.")
                summary["status"] = "partial_staged"
        else:
            summary["status"] = "dry_run_completed"
            if all_completed:
                cp_mgr.clear_checkpoint_only()

        summary["elapsed_sec"] = round(time.monotonic() - started_monotonic, 2)
        summary["finished_at_utc"] = utc_now_iso()
        cp_mgr.write_summary(summary)
        log.info("Sync finished cleanly in %.1fs.", summary["elapsed_sec"])
        return 0

    except Exception as exc:
        log.exception("Sync interrupted: %s", exc)
        # Preserve checkpoint so the next scheduled/manual run resumes seamlessly.
        # Live WordPress data remains 100% untouched because commit was never sent.
        summary["status"] = "interrupted_checkpoint_saved"
        summary["error"] = str(exc)
        summary["elapsed_sec"] = round(time.monotonic() - started_monotonic, 2)
        summary["finished_at_utc"] = utc_now_iso()
        cp_mgr.build_emergency_export(state)
        cp_mgr.write_summary(summary)
        return 1
    finally:
        if uploader is not None:
            uploader.close()


if __name__ == "__main__":
    raise SystemExit(run_sync(build_arg_parser().parse_args()))
