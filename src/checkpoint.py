"""Atomic Checkpoint & Resume manager plus local JSON snapshot persistence."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .models import CheckpointState, IngredientRecord, ListProgress


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class CheckpointManager:
    def __init__(self, state_dir: Path, ttl_hours: int = 36) -> None:
        self.state_dir = state_dir
        self.ttl_hours = ttl_hours
        self.checkpoint_path = self.state_dir / "checkpoint.json"
        self.lists_cache_dir = self.state_dir / "lists_cache"
        self.export_path = self.state_dir / "sfda_emergency_export.json"
        self.summary_path = self.state_dir / "sync_summary.json"

        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.lists_cache_dir.mkdir(parents=True, exist_ok=True)

    def _atomic_write_json(self, path: Path, payload: Any) -> None:
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp_path, path)

    def load_or_create(
        self,
        scope_slugs: tuple[str, ...],
        scope_label: str,
        *,
        force_reset: bool = False,
    ) -> tuple[CheckpointState, bool]:
        """
        Return `(state, resumed)`. Reuses an unfinished checkpoint if it matches the scope
        and has not exceeded TTL; otherwise initializes a clean new sync session.
        """
        if not force_reset and self.checkpoint_path.exists():
            try:
                raw = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
                state = CheckpointState.from_dict(raw)
                updated_dt = datetime.fromisoformat(state.updated_at_utc)
                age = datetime.now(timezone.utc) - updated_dt
                if state.scope_label == scope_label and age <= timedelta(hours=self.ttl_hours):
                    for slug in scope_slugs:
                        if slug not in state.lists:
                            state.lists[slug] = ListProgress(slug=slug)
                    return state, True
            except Exception:
                pass

        self.clear_all()
        now = utc_now_iso()
        session_uuid = f"sfda-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:10]}"
        state = CheckpointState(
            session_uuid=session_uuid,
            scope_label=scope_label,
            created_at_utc=now,
            updated_at_utc=now,
            session_started_on_wp=False,
            lists={slug: ListProgress(slug=slug) for slug in scope_slugs},
        )
        self.save(state)
        return state, False

    def save(self, state: CheckpointState) -> None:
        state.updated_at_utc = utc_now_iso()
        self._atomic_write_json(self.checkpoint_path, state.to_dict())

    def append_list_records(self, list_slug: str, records: list[IngredientRecord]) -> None:
        """Append scraped page records to JSONL cache for reliable emergency export."""
        if not records:
            return
        cache_file = self.lists_cache_dir / f"{list_slug}.jsonl"
        with cache_file.open("a", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec.to_export_dict(), ensure_ascii=False) + "\n")

    def build_emergency_export(self, state: CheckpointState) -> Path:
        """
        Consolidate all cached JSONL list files into one deduplicated JSON export
        compatible with Stage 5's WP Admin Emergency JSON Importer.
        """
        items_by_list: dict[str, dict[str, dict[str, Any]]] = {}
        total_items = 0

        for slug in state.lists:
            cache_file = self.lists_cache_dir / f"{slug}.jsonl"
            deduped: dict[str, dict[str, Any]] = {}
            if cache_file.exists():
                with cache_file.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                            skey = str(obj.get("source_key", ""))
                            if skey:
                                deduped[skey] = obj
                        except json.JSONDecodeError:
                            continue
            items_by_list[slug] = deduped
            total_items += len(deduped)

        export_payload = {
            "schema_version": "1.0",
            "generated_at_utc": utc_now_iso(),
            "session_uuid": state.session_uuid,
            "scope": state.scope_label,
            "total_items": total_items,
            "counts_by_list": {slug: len(rows) for slug, rows in items_by_list.items()},
            "lists": {slug: list(rows.values()) for slug, rows in items_by_list.items()},
        }
        self._atomic_write_json(self.export_path, export_payload)
        return self.export_path

    def write_summary(self, summary: dict[str, Any]) -> Path:
        self._atomic_write_json(self.summary_path, summary)
        return self.summary_path

    def clear_checkpoint_only(self) -> None:
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()

    def clear_all(self) -> None:
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()
        for f in self.lists_cache_dir.glob("*.jsonl"):
            f.unlink()
