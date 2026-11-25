"""
HMAC-SHA256 signed client for the WordPress SFDA Mirror Sync REST API.
Matches `Hmac_Authenticator` and `Sync_Rest_Controller` (including Deletion Circuit Breaker).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import random
import time
from typing import Any

import httpx

from .models import IngredientRecord

logger = logging.getLogger(__name__)

RETRYABLE_WP_STATUS = {408, 425, 429, 500, 502, 503, 504}


class WordPressUploader:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        hmac_secret: str,
        timeout_sec: float = 45.0,
        max_retries: int = 4,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.hmac_secret = hmac_secret.strip()
        self.max_retries = max_retries
        self._client = httpx.Client(
            timeout=httpx.Timeout(timeout_sec, connect=20.0),
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> WordPressUploader:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _sign(self, raw_body: str, timestamp: int) -> str:
        message = f"{timestamp}.{raw_body}".encode("utf-8")
        return hmac.new(self.hmac_secret.encode("utf-8"), message, hashlib.sha256).hexdigest()

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        raw_body = (
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            if payload is not None
            else ""
        )
        body_bytes = raw_body.encode("utf-8") if raw_body else None

        last_err: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            ts = int(time.time())
            signature = self._sign(raw_body, ts)
            headers = {
                "X-SFDA-API-Key": self.api_key,
                "X-SFDA-Timestamp": str(ts),
                "X-SFDA-Signature": signature,
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "application/json",
                "User-Agent": "SFDA-Sync-Worker/1.1",
            }

            try:
                resp = self._client.request(method, url, content=body_bytes, headers=headers)
                if resp.status_code in RETRYABLE_WP_STATUS:
                    raise httpx.HTTPStatusError(
                        f"Retryable WP status {resp.status_code}: {resp.text[:300]}",
                        request=resp.request,
                        response=resp,
                    )

                ctype = resp.headers.get("content-type", "").lower()
                if "application/json" not in ctype:
                    raise RuntimeError(
                        f"Unexpected non-JSON response from WordPress ({resp.status_code}, {ctype}): {resp.text[:250]}"
                    )

                data = resp.json()
                if resp.status_code >= 400:
                    err_code = data.get("code", f"http_{resp.status_code}")
                    err_msg = data.get("message", resp.text[:300])
                    raise RuntimeError(f"WordPress API rejected request [{err_code}] (HTTP {resp.status_code}): {err_msg}")

                return data
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                last_err = exc
                if attempt >= self.max_retries:
                    break
                backoff = min(30.0, (2.0 ** attempt) + random.uniform(0.2, 1.0))
                logger.warning(
                    "WP API attempt %d/%d failed for %s %s (%s). Retrying in %.1fs...",
                    attempt,
                    self.max_retries,
                    method,
                    path,
                    exc,
                    backoff,
                )
                time.sleep(backoff)

        raise RuntimeError(f"WordPress API failed after {self.max_retries} retries on {method} {path}: {last_err}")

    def check_status(self) -> dict[str, Any]:
        return self._request("GET", "/status")

    def start_session(self, session_uuid: str, trigger_source: str, lists_scope: str) -> dict[str, Any]:
        return self._request(
            "POST",
            "/sessions/start",
            {
                "session_uuid": session_uuid,
                "trigger_source": trigger_source,
                "lists_scope": lists_scope,
            },
        )

    def push_batch(self, session_uuid: str, list_slug: str, records: list[IngredientRecord]) -> dict[str, Any]:
        return self._request(
            "POST",
            "/sessions/batch",
            {
                "session_uuid": session_uuid,
                "list_slug": list_slug,
                "items": [rec.to_wp_payload() for rec in records],
            },
        )

    def commit_session(
        self,
        session_uuid: str,
        completed_lists: list[str],
        expected_counts: dict[str, int],
        *,
        failed_pages: dict[str, int] | None = None,
        force_commit: bool = False,
        allow_mass_delete: bool = False,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/sessions/commit",
            {
                "session_uuid": session_uuid,
                "completed_lists": completed_lists,
                "expected_counts": expected_counts,
                "failed_pages": failed_pages or {},
                "force_commit": force_commit,
                "allow_mass_delete": allow_mass_delete,
            },
        )

    def abort_session(self, session_uuid: str, reason: str) -> dict[str, Any]:
        return self._request(
            "POST",
            "/sessions/abort",
            {
                "session_uuid": session_uuid,
                "reason": reason[:1000],
            },
        )
