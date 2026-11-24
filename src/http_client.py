"""
Fault-tolerant HTTPX client with thread-safe rate limiting, randomized jitter,
exponential backoff, and soft-block validation.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from typing import Callable

import httpx

from .config import BROWSER_USER_AGENT

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504, 520, 522, 524}


class SoftBlockError(RuntimeError):
    """Raised when SFDA returns HTTP 200 with an empty/WAF-challenged HTML shell."""


class ResilientHttpClient:
    def __init__(
        self,
        *,
        timeout_sec: float = 45.0,
        max_retries: int = 5,
        min_delay_sec: float = 0.45,
        max_connections: int = 8,
    ) -> None:
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.min_delay_sec = min_delay_sec
        self._rate_lock = threading.Lock()
        self._last_request_monotonic = 0.0

        self._client = httpx.Client(
            headers={
                "User-Agent": BROWSER_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
            },
            timeout=httpx.Timeout(timeout_sec, connect=20.0),
            limits=httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_connections),
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> ResilientHttpClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _wait_rate_limit(self) -> None:
        with self._rate_lock:
            now = time.monotonic()
            jitter = random.uniform(0.0, 0.25)
            target_gap = self.min_delay_sec + jitter
            elapsed = now - self._last_request_monotonic
            if elapsed < target_gap:
                time.sleep(target_gap - elapsed)
            self._last_request_monotonic = time.monotonic()

    def get_text(
        self,
        url: str,
        *,
        validator: Callable[[str], bool] | None = None,
        referer: str | None = None,
    ) -> str:
        """
        Fetch a URL with rate limiting, exponential backoff, and optional HTML content
        validation to defeat silent HTTP-200 soft blocks.
        """
        extra_headers = {"Referer": referer} if referer else None
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            self._wait_rate_limit()
            try:
                resp = self._client.get(url, headers=extra_headers)
                if resp.status_code in RETRYABLE_STATUS_CODES:
                    raise httpx.HTTPStatusError(
                        f"Retryable HTTP status {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                resp.raise_for_status()

                html = resp.text
                if validator is not None and not validator(html):
                    raise SoftBlockError(
                        f"HTTP 200 received for {url} (len={len(html)}) but failed HTML integrity check."
                    )
                return html
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError, SoftBlockError) as exc:
                last_error = exc
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code not in RETRYABLE_STATUS_CODES:
                    raise
                if attempt >= self.max_retries:
                    break
                backoff = min(60.0, (2.0 ** attempt) + random.uniform(0.3, 1.8))
                logger.warning(
                    "Attempt %d/%d failed for %s (%s). Backing off %.1fs...",
                    attempt,
                    self.max_retries,
                    url,
                    exc,
                    backoff,
                )
                time.sleep(backoff)

        raise RuntimeError(f"Exhausted {self.max_retries} retries for {url}: {last_error}") from last_error
