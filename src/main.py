#!/usr/bin/env python3
"""
Entrypoint for the Resilient SFDA Cosmetic Mirror Sync Worker.
Orchestrates checkpoint loading, polite paginated scraping, strict page-completeness
verification, batch staging, emergency JSON export generation, and atomic WordPress commit.
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
        help="Override completeness/drop guards (only if an intentional major drop occurred).",
    )
    parser.add_argument(
        "--allow-mass-delete",
        action="store_true",
        help="Allow WordPress Deletion Circuit Breaker to delete more than the safe threshold.",
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


def _max_safe_drop(live_count: int) -> int:
    """Mirror PHP Diff_Engine::calculate_max_safe_removals() (< 1 SFDA page of 10 rows)."""
    if live_count <= 0:
        return 0
    return max(3, min(9, int(live_count * 0.02)))


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
        "incomplete_lists": {},
    }

    live_counts_by_slug: dict[str, int] = {}
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
            for item in wp_status.get("lists") or []:
                slug_k = str(item.get("slug") or "").strip()
                if slug_k:
                    live_counts_by_slug[slug_k] = int(item.get("items_count") or 0)

            log.info(
                "Connected to WordPress Mirror v%s (live items: %s | per-list: %s)",
                wp_status.get("plugin_version", "?"),
                wp_status.get("total_live_items", "?"),
                live_counts_by_slug,
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
                    log.info(
                        "Skipping [%s] — already completed in checkpoint (%d items, failed_pages=%d).",
                        slug,
                        prog.items_scraped,
                        len(prog.failed_pages),
                    )
                    summary["lists"][slug] = {
                        "items_scraped": prog.items_scraped,
                        "total_pages": prog.total_pages,
                        "failed_pages": prog.failed_pages,
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
                    prior_failed_pages=prog.failed_pages,
                ):
                    prog.total_pages = total_pages
                    cp_mgr.append_list_records(slug, page_records)
                    batch_buffer.extend(page_records)

                    is_last_page = (page_num + 1) >= total_pages
                    if len(batch_buffer) >= config.batch_size or is_last_page or config.max_pages_per_list is not None:
                        flush_batch(uploader, state.session_uuid, slug, batch_buffer, config.batch_size)

                    # Never regress last_completed_page when rescue pass yields an earlier page index.
                    prog.last_completed_page = max(prog.last_completed_page, page_num)
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

                flush_batch(uploader, state.session_uuid, slug, batch_buffer, config.batch_size)

                prog.failed_pages = list(scraper.unrecovered_empty_pages.get(slug, []))
                if config.max_pages_per_list is None:
                    prog.completed = True
                    cp_mgr.save(state)

                summary["lists"][slug] = {
                    "items_scraped": prog.items_scraped,
                    "total_pages": prog.total_pages,
                    "failed_pages": prog.failed_pages,
                    "completed": prog.completed,
                }

        # Always generate the emergency JSON backup file.
        export_file = cp_mgr.build_emergency_export(state)
        log.info("Emergency JSON backup written to: %s", export_file)

        # Strict Page-Completeness & Anti-Drop Gate per list before WordPress commit.
        eligible_commit_lists: list[str] = []
        incomplete_lists: dict[str, dict[str, Any]] = {}

        for slug in config.lists_scope:
            prog = state.lists[slug]
            if not prog.completed:
                incomplete_lists[slug] = {"reason": "pagination_not_finished"}
                continue

            live_cnt = live_counts_by_slug.get(slug, 0)
            drop_delta = live_cnt - prog.items_scraped
            max_drop = _max_safe_drop(live_cnt)

            if prog.failed_pages and not config.force_commit:
                incomplete_lists[slug] = {
                    "reason": "unrecovered_empty_pages",
                    "failed_pages": prog.failed_pages,
                    "items_scraped": prog.items_scraped,
                    "live_count": live_cnt,
                }
                log.error(
                    "BLOCKING COMMIT for [%s]: %d unrecovered empty page(s) %s. Live WordPress data kept intact.",
                    slug,
                    len(prog.failed_pages),
                    prog.failed_pages,
                )
                continue

            if live_cnt > 0 and drop_delta > max_drop and not config.force_commit:
                incomplete_lists[slug] = {
                    "reason": "anomalous_count_drop",
                    "items_scraped": prog.items_scraped,
                    "live_count": live_cnt,
                    "drop_delta": drop_delta,
                    "max_safe_drop": max_drop,
                }
                log.error(
                    "BLOCKING COMMIT for [%s]: scraped %d vs live %d (drop of %d exceeds safe threshold %d). "
                    "Likely truncated SFDA pagination — live WordPress data kept intact.",
                    slug,
                    prog.items_scraped,
                    live_cnt,
                    drop_delta,
                    max_drop,
                )
                continue

            eligible_commit_lists.append(slug)

        summary["incomplete_lists"] = incomplete_lists

        exit_code = 0
        if not config.dry_run and uploader is not None:
            if config.max_pages_per_list is not None:
                log.warning("Smoke test mode (--max-pages-per-list); aborting staging without committing.")
                uploader.abort_session(state.session_uuid, "Smoke test cap active")
                cp_mgr.clear_checkpoint_only()
                summary["status"] = "partial_staged"
            elif eligible_commit_lists:
                expected_counts = {s: state.lists[s].items_scraped for s in eligible_commit_lists}
                failed_pages_counts = {s: len(state.lists[s].failed_pages) for s in eligible_commit_lists}
                commit_res = uploader.commit_session(
                    session_uuid=state.session_uuid,
                    completed_lists=eligible_commit_lists,
                    expected_counts=expected_counts,
                    failed_pages=failed_pages_counts,
                    force_commit=config.force_commit,
                    allow_mass_delete=bool(args.allow_mass_delete),
                )
                log.info(
                    "ATOMIC COMMIT SUCCEEDED for %s: %s (deletions_blocked=%s)",
                    eligible_commit_lists,
                    commit_res.get("stats"),
                    commit_res.get("deletions_blocked", 0),
                )
                if commit_res.get("protected_lists"):
                    log.warning("WordPress Deletion Circuit Breaker protected: %s", commit_res["protected_lists"])

                summary["commit_result"] = commit_res
                cp_mgr.clear_checkpoint_only()

                if incomplete_lists:
                    summary["status"] = "partial_committed_incomplete_lists_skipped"
                    log.error(
                        "Skipped committing incomplete list(s): %s. Re-run for those list(s) to sync them safely.",
                        list(incomplete_lists.keys()),
                    )
                    exit_code = 1
                else:
                    summary["status"] = "committed"
            else:
                reason = f"Strict completeness gate blocked all lists: {list(incomplete_lists.keys())}"
                log.error("%s — aborting WP staging so live database remains 100%% untouched.", reason)
                uploader.abort_session(state.session_uuid, reason)
                cp_mgr.clear_checkpoint_only()
                summary["status"] = "aborted_incomplete_scrape"
                exit_code = 1
        else:
            summary["status"] = "dry_run_completed" if not incomplete_lists else "dry_run_incomplete"
            cp_mgr.clear_checkpoint_only()
            if incomplete_lists:
                exit_code = 1

        summary["elapsed_sec"] = round(time.monotonic() - started_monotonic, 2)
        summary["finished_at_utc"] = utc_now_iso()
        cp_mgr.write_summary(summary)
        log.info("Sync finished in %.1fs with status=%s (exit=%d).", summary["elapsed_sec"], summary["status"], exit_code)
        return exit_code

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
