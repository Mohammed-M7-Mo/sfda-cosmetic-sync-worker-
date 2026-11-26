"""
HMAC-SHA256 signed client for the WordPress SFDA Circulars Sync REST API.
Automatically derives `/wp-json/sfda-mirror/v1/circulars/sync` from `WP_SYNC_BASE_URL`.
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

logger = logging.getLogger(__name__)

RETRYABLE_WP_STATUS = {408, 425, 429, 500, 502, 503, 504}


def derive_circulars_base_url(wp_sync_base_url: str) -> str:
    """
    Convert `https://example.com/wp-json/sfda-mirror/v1/sync`
    into    `https://example.com/wp-json/sfda-mirror/v1/circulars/sync`.
    """
    clean = wp_sync_base_url.strip().rstrip("/")
    if clean.endswith("/circulars/sync"):
        return clean
    if clean.endswith("/sync"):
        return clean[: -len("/sync")] + "/circulars/sync"
    return clean + "/circulars/sync"


class CircularsUploader:
    def __init__(
        self,
        *,
        wp_sync_base_url: str,
        api_key: str,
        hmac_secret: str,
        timeout_sec: float = 45.0,
        max_retries: int = 4,
    ) -> None:
        self.base_url = derive_circulars_base_url(wp_sync_base_url)
        self.api_key = api_key.strip()
        self.hmac_secret = hmac_secret.strip()
        self.max_retries = max_retries
        self._client = httpx.Client(
            timeout=httpx.Timeout(timeout_sec, connect=20.0),
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> CircularsUploader:
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
                "User-Agent": "SFDA-Circulars-Worker/1.0",
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
                    raise RuntimeError(
                        f"WordPress Circulars API rejected request [{err_code}] (HTTP {resp.status_code}): {err_msg}"
                    )

                return data
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                last_err = exc
                if attempt >= self.max_retries:
                    break
                backoff = min(30.0, (2.0 ** attempt) + random.uniform(0.2, 1.0))
                logger.warning(
                    "WP Circulars API attempt %d/%d failed for %s %s (%s). Retrying in %.1fs...",
                    attempt,
                    self.max_retries,
                    method,
                    path,
                    exc,
                    backoff,
                )
                time.sleep(backoff)

        raise RuntimeError(f"WP Circulars API failed after {self.max_retries} retries on {method} {path}: {last_err}")

    def check_status(self) -> dict[str, Any]:
        return self._request("GET", "/status")

    def push_batch(
        self,
        session_id: str,
        items: list[dict[str, Any]],
        *,
        is_baseline: bool = False,
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/batch",
            {
                "session_id": session_id,
                "is_baseline": is_baseline,
                "items": items,
            },
        )

    def finish_sync(self, session_id: str, *, is_baseline: bool = False) -> dict[str, Any]:
        return self._request(
            "POST",
            "/finish",
            {
                "session_id": session_id,
                "is_baseline": is_baseline,
            },
        )