"""
Canonical cleaning, normalization, source_key generation, and SHA-256 hashing.
Guaranteed 100% parity with PHP `Diff_Engine::compute_row_hash()` in Stage 3.
"""

from __future__ import annotations

import hashlib
import re
from typing import Mapping

TRACKED_FIELDS: tuple[str, ...] = (
    "reference_number",
    "chemical_name",
    "inci_name",
    "cas_number",
    "ec_number",
    "product_type_body_parts",
    "max_concentration",
    "other_conditions",
    "warnings_conditions",
)

_WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)
_CAS_TOKEN_RE = re.compile(r"\b\d{2,7}-\d{2}-\d\b")


def clean_field(value: object | None) -> str:
    """Mirror PHP Diff_Engine::clean_field() exactly."""
    if value is None:
        return ""
    text = str(value).strip()
    if text == "" or text == "-":
        return ""
    return _WHITESPACE_RE.sub(" ", text).strip()


def normalize_name(value: str) -> str:
    """Mirror PHP Ingredient_Repository::normalize_name()."""
    cleaned = clean_field(value)
    if not cleaned:
        return ""
    return cleaned.upper()[:191]


def normalize_cas_tokens(cas_raw: str) -> str:
    """Mirror PHP Ingredient_Repository::normalize_cas_tokens()."""
    trimmed = clean_field(cas_raw)
    if not trimmed:
        return ""

    matches = _CAS_TOKEN_RE.findall(trimmed)
    tokens: list[str] = []
    if matches:
        for cas in matches:
            if cas not in tokens:
                tokens.append(cas)
    else:
        for part in re.split(r"[/,;|\s]+", trimmed):
            p = part.strip().upper()
            if p and p != "-" and p not in tokens:
                tokens.append(p)

    if not tokens:
        return ""
    return ("|" + "|".join(tokens) + "|")[:191]


def compute_row_hash(fields: Mapping[str, str]) -> str:
    """
    Compute deterministic SHA-256 fingerprint over the 9 tracked scientific fields
    joined by ASCII Unit Separator (\\x1F), identical to PHP Diff_Engine::compute_row_hash().
    """
    parts = [clean_field(fields.get(key, "")) for key in TRACKED_FIELDS]
    canonical = "\x1f".join(parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_source_key(
    list_slug: str,
    reference_number: str,
    cas_number: str,
    chemical_name: str,
    used_keys_in_list: set[str],
) -> str:
    """
    Build a stable, unique source_key within a list.
    Prefers regulatory Reference Number + CAS (so edits to concentration/warnings
    produce clean `updated` diffs instead of delete+add), while handling empty CAS
    or duplicate reference numbers safely.
    """
    ref = clean_field(reference_number)
    norm_cas = normalize_cas_tokens(cas_number)
    norm_name = normalize_name(chemical_name)

    if ref and norm_cas:
        seed = f"{list_slug}|ref:{ref}|cas:{norm_cas}"
    elif ref and norm_name:
        seed = f"{list_slug}|ref:{ref}|name:{norm_name}"
    elif norm_cas and norm_name:
        seed = f"{list_slug}|cas:{norm_cas}|name:{norm_name}"
    else:
        seed = f"{list_slug}|name:{norm_name}"

    base_key = f"{list_slug}_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:32]}"
    candidate = base_key
    suffix = 2
    while candidate in used_keys_in_list:
        candidate = f"{base_key}_{suffix}"
        suffix += 1

    used_keys_in_list.add(candidate)
    return candidate[:191]
