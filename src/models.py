"""Typed data models for scraped cosmetic ingredients and checkpoint state."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class IngredientRecord:
    list_slug: str
    source_key: str
    reference_number: str = ""
    chemical_name: str = ""
    inci_name: str = ""
    cas_number: str = ""
    ec_number: str = ""
    product_type_body_parts: str = ""
    max_concentration: str = ""
    other_conditions: str = ""
    warnings_conditions: str = ""
    row_hash: str = ""
    raw_details: dict[str, str] = field(default_factory=dict)
    source_page: int = 0

    def to_wp_payload(self) -> dict[str, Any]:
        """Serialize in the exact schema expected by WP Sync_Rest_Controller."""
        return {
            "source_key": self.source_key,
            "reference_number": self.reference_number,
            "chemical_name": self.chemical_name,
            "inci_name": self.inci_name,
            "cas_number": self.cas_number,
            "ec_number": self.ec_number,
            "product_type_body_parts": self.product_type_body_parts,
            "max_concentration": self.max_concentration,
            "other_conditions": self.other_conditions,
            "warnings_conditions": self.warnings_conditions,
            "row_hash": self.row_hash,
            "raw_details": self.raw_details,
            "source_page": self.source_page,
        }

    def to_export_dict(self) -> dict[str, Any]:
        data = self.to_wp_payload()
        data["list_slug"] = self.list_slug
        return data


@dataclass
class ListProgress:
    slug: str
    total_pages: int | None = None
    last_completed_page: int = -1
    items_scraped: int = 0
    completed: bool = False
    failed_pages: list[int] = field(default_factory=list)
    used_source_keys: list[str] = field(default_factory=list)


@dataclass
class CheckpointState:
    session_uuid: str
    scope_label: str
    created_at_utc: str
    updated_at_utc: str
    session_started_on_wp: bool = False
    lists: dict[str, ListProgress] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_uuid": self.session_uuid,
            "scope_label": self.scope_label,
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "session_started_on_wp": self.session_started_on_wp,
            "lists": {slug: asdict(prog) for slug, prog in self.lists.items()},
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> CheckpointState:
        lists_map: dict[str, ListProgress] = {}
        for slug, item in (raw.get("lists") or {}).items():
            lists_map[slug] = ListProgress(
                slug=slug,
                total_pages=item.get("total_pages"),
                last_completed_page=int(item.get("last_completed_page", -1)),
                items_scraped=int(item.get("items_scraped", 0)),
                completed=bool(item.get("completed", False)),
                failed_pages=[int(p) for p in (item.get("failed_pages") or [])],
                used_source_keys=list(item.get("used_source_keys") or []),
            )
        return cls(
            session_uuid=str(raw["session_uuid"]),
            scope_label=str(raw.get("scope_label", "all")),
            created_at_utc=str(raw["created_at_utc"]),
            updated_at_utc=str(raw["updated_at_utc"]),
            session_started_on_wp=bool(raw.get("session_started_on_wp", False)),
            lists=lists_map,
        )
