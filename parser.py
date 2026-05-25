"""
Twitter wallet mention parser.
Multi-Nitter pool, persistent HTTP clients per worker, parallel Nitter+API
consumers (sum, not max), passive cold-wait, async DB writes.
Backward-compatible config / state.db / accounts.json.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
import signal
import sqlite3
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from logging.handlers import RotatingFileHandler
from collections import Counter
from pathlib import Path
from urllib.parse import quote, urlparse

import httpx

# Heavy parser deps — lazy-loaded so --bot mode starts with just httpx.
bs4 = None  # type: ignore
requests = None  # type: ignore
ClientTransaction = None  # type: ignore
generate_headers = None  # type: ignore
get_ondemand_file_url = None  # type: ignore
cc_requests = None  # type: ignore
_HAS_CURL_CFFI = False


def _load_parser_deps():
    """Import heavy dependencies needed for the actual parser engine."""
    global bs4, requests, ClientTransaction, generate_headers
    global get_ondemand_file_url, cc_requests, _HAS_CURL_CFFI
    if bs4 is not None:
        return
    import importlib
    bs4 = importlib.import_module("bs4")
    requests = importlib.import_module("requests")
    _xt = importlib.import_module("x_client_transaction")
    ClientTransaction = _xt.ClientTransaction
    _xtu = importlib.import_module("x_client_transaction.utils")
    generate_headers = _xtu.generate_headers
    get_ondemand_file_url = _xtu.get_ondemand_file_url
    try:
        _cc = importlib.import_module("curl_cffi")
        # curl_cffi >= 0.7 moved requests into curl_cffi.requests submodule
        if hasattr(_cc, "requests"):
            cc_requests = _cc.requests
        else:
            cc_requests = importlib.import_module("curl_cffi.requests")
        _HAS_CURL_CFFI = True
    except (ImportError, AttributeError):
        _HAS_CURL_CFFI = False


BASE = Path(__file__).parent
ACCOUNTS_FILE = BASE / "accounts.json"
CONFIG_FILE = BASE / "config.json"
INPUT_WALLETS = BASE / "input" / "wallets.txt"
INPUT_DEDUP = BASE / "input" / "bazaTwitters.txt"
OUTPUT_RESULTS = BASE / "output" / "results.txt"
OUTPUT_ERRORS = BASE / "output" / "errors.log"
STATE_DB = BASE / "state.db"
BUILD_TAG = "ready-20260524-v10-tg-bot-beautiful"

# Shared runtime state — amain() publishes here, TgBotController reads.
_RUNTIME: dict = {}


def _h(text) -> str:
    """Escape HTML special chars for Telegram HTML parse_mode."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ---- logging ---------------------------------------------------------------

log = logging.getLogger("parser")


def _setup_logging(errors_path: Path):
    """Timestamped stdout (INFO+) + rotating errors.log (WARNING+)."""
    log.setLevel(logging.DEBUG)
    for h in list(log.handlers):
        log.removeHandler(h)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)
    errors_path.parent.mkdir(parents=True, exist_ok=True)
    fh = RotatingFileHandler(
        str(errors_path),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.WARNING)
    fh.setFormatter(fmt)
    log.addHandler(fh)
    log.propagate = False


BEARER = (
    "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D"
    "1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]


def _pick_ua(label: str) -> str:
    """Stable UA per worker (md5 → consistent across restarts)."""
    idx = int(hashlib.md5(label.encode("utf-8")).hexdigest(), 16) % len(UA_POOL)
    return UA_POOL[idx]


QUERY_ID = "rkp6b4vtR9u7v3naGoOzUQ"
SEARCH_URL = f"https://x.com/i/api/graphql/{QUERY_ID}/SearchTimeline"
_QUERY_REFRESH_LOCK = None
_QUERY_REFRESH_LAST_MONO = 0.0
_API_CIRCUIT_OPEN_UNTIL_MONO = 0.0
_API_404_STREAK = 0
_API_CIRCUIT_LAST_LOG_MONO = 0.0



def _scrape_query_id_attempt(proxy_url, timeout=20):
    """Scrape current SearchTimeline query_id from X web assets.

    The old version only inspected the first main.*.js and one exact minified
    pattern. When X changes chunk layout/minifier order, startup may keep a
    stale hardcoded query_id and every API request turns into 404. This version
    scans several client-web JS assets and accepts queryId before/after
    operationName.
    """
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA_POOL[0],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    if proxy_url:
        s.proxies = {"http": proxy_url, "https": proxy_url}

    home = s.get("https://x.com", timeout=timeout, allow_redirects=True)
    if home.status_code != 200:
        raise RuntimeError(f"home fetch status={home.status_code}")

    js_urls = []
    # Absolute client-web bundles.
    js_urls.extend(re.findall(
        r"https://abs\.twimg\.com/responsive-web/client-web/[^\"']+?\.js",
        home.text,
    ))
    # Relative/script src fallbacks, just in case X emits relative URLs.
    for u in re.findall(r'<script[^>]+src=["\']([^"\']+\.js)["\']', home.text):
        if u.startswith("https://abs.twimg.com/responsive-web/client-web/"):
            js_urls.append(u)
        elif u.startswith("/responsive-web/client-web/"):
            js_urls.append("https://abs.twimg.com" + u)

    # Prefer main first, but scan more than one bundle.
    js_urls = list(dict.fromkeys(js_urls))
    js_urls.sort(key=lambda u: (0 if "/main." in u else 1, u))
    if not js_urls:
        raise RuntimeError("client-web JS URLs not found in home page HTML")

    patterns = [
        # queryId before operationName
        re.compile(r'queryId:"([^"]+)",operationName:"SearchTimeline"'),
        re.compile(r'"queryId":"([^"]+)","operationName":"SearchTimeline"'),
        # operationName before queryId
        re.compile(r'operationName:"SearchTimeline",queryId:"([^"]+)"'),
        re.compile(r'"operationName":"SearchTimeline","queryId":"([^"]+)"'),
        # tolerant bounded search in an object-ish chunk
        re.compile(r'SearchTimeline.{0,400}?queryId["\']?\s*[:=]\s*["\']([^"\']+)["\']'),
        re.compile(r'queryId["\']?\s*[:=]\s*["\']([^"\']+)["\'].{0,400}?SearchTimeline'),
    ]

    checked = 0
    last_status = None
    for url in js_urls[:24]:
        checked += 1
        js_resp = s.get(url, timeout=timeout, allow_redirects=True)
        last_status = js_resp.status_code
        if js_resp.status_code != 200:
            continue
        text = js_resp.text
        for pat in patterns:
            m = pat.search(text)
            if m:
                return m.group(1)

    raise RuntimeError(
        f"SearchTimeline queryId not found after scanning {checked} JS assets "
        f"(last_status={last_status})"
    )


def scrape_search_timeline_query_id(proxy_candidates, timeout=20):
    """Try each proxy + direct. Return query_id string or None."""
    candidates = list(proxy_candidates) + [None]
    last_err = None
    for idx, p in enumerate(candidates[:4]):
        label = p.split("@")[1] if p and "@" in p else (p or "direct")
        try:
            qid = _scrape_query_id_attempt(p, timeout=timeout)
            log.info(f"[setup] query_id scraped via {label}: {qid}")
            return qid
        except Exception as e:
            last_err = e
            log.warning(f"[setup] scrape attempt {idx+1} via {label} failed: {e}")
            continue
    log.error(f"[setup] ALL scrape attempts failed, last_err={last_err}")
    return None


async def refresh_search_timeline_query_id(proxy_url, reason="api 404"):
    """Refresh SearchTimeline query_id once per minute across all workers."""
    global QUERY_ID, SEARCH_URL, _QUERY_REFRESH_LOCK, _QUERY_REFRESH_LAST_MONO, _API_CIRCUIT_OPEN_UNTIL_MONO, _API_404_STREAK
    if _QUERY_REFRESH_LOCK is None:
        _QUERY_REFRESH_LOCK = asyncio.Lock()
    async with _QUERY_REFRESH_LOCK:
        now = time.monotonic()
        if now - _QUERY_REFRESH_LAST_MONO < 60:
            return "recent"
        _QUERY_REFRESH_LAST_MONO = now
        old_qid = QUERY_ID
        loop = asyncio.get_running_loop()
        qid = await loop.run_in_executor(
            None,
            lambda: scrape_search_timeline_query_id([proxy_url], timeout=12),
        )
        if not qid:
            log.warning(f"[setup] query_id refresh failed after {reason}")
            return "failed"
        if qid != old_qid:
            QUERY_ID = qid
            SEARCH_URL = f"https://x.com/i/api/graphql/{QUERY_ID}/SearchTimeline"
            _API_404_STREAK = 0
            _API_CIRCUIT_OPEN_UNTIL_MONO = 0.0
            log.warning(f"[setup] query_id refreshed after {reason}: {old_qid} -> {qid}")
            return "updated"
        log.warning(f"[setup] query_id refresh after {reason}: unchanged {qid}")
        return "same"


def api_circuit_sleep_left() -> float:
    """Seconds until API circuit reopens. 0 means API can be tried."""
    return max(0.0, _API_CIRCUIT_OPEN_UNTIL_MONO - time.monotonic())


def api_circuit_note_success():
    """Reset global API 404 circuit after any successful API response."""
    global _API_404_STREAK, _API_CIRCUIT_OPEN_UNTIL_MONO, _API_CIRCUIT_LAST_LOG_MONO
    _API_404_STREAK = 0
    _API_CIRCUIT_OPEN_UNTIL_MONO = 0.0
    _API_CIRCUIT_LAST_LOG_MONO = 0.0


def api_circuit_note_404(cfg: dict, label: str, status: str):
    """Open a global circuit if SearchTimeline keeps returning stale 404.

    This prevents all API consumers from taking wallets from the queue, failing,
    then hammering Nitter fallback. When the endpoint/query_id is bad globally,
    parking API workers is faster and produces far fewer errors.
    """
    global _API_404_STREAK, _API_CIRCUIT_OPEN_UNTIL_MONO, _API_CIRCUIT_LAST_LOG_MONO
    _API_404_STREAK += 1
    threshold = max(1, int(cfg.get("api_404_circuit_threshold", 6)))
    cooldown = max(30.0, float(cfg.get("api_404_circuit_cooldown_s", 600)))
    if _API_404_STREAK >= threshold:
        now = time.monotonic()
        was_open = _API_CIRCUIT_OPEN_UNTIL_MONO > now
        _API_CIRCUIT_OPEN_UNTIL_MONO = max(
            _API_CIRCUIT_OPEN_UNTIL_MONO,
            now + cooldown,
        )
        if (not was_open) or (now - _API_CIRCUIT_LAST_LOG_MONO >= 300):
            _API_CIRCUIT_LAST_LOG_MONO = now
            log.warning(
                f"[api-circuit] OPEN for {cooldown:.0f}s after {_API_404_STREAK} "
                f"SearchTimeline 404s (last={label}, refresh={status}). "
                f"API workers will pause instead of wasting requests/fallbacks."
            )


FEATURES = {
    "rweb_video_screen_enabled": False,
    "profile_label_improvements_pcf_label_in_post_enabled": True,
    "responsive_web_profile_redirect_enabled": False,
    "rweb_tipjar_consumption_enabled": False,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "premium_content_api_read_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
    "responsive_web_grok_analyze_post_followups_enabled": True,
    "responsive_web_jetfuel_frame": True,
    "responsive_web_grok_share_attachment_enabled": True,
    "responsive_web_grok_annotations_enabled": False,
    "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "responsive_web_grok_show_grok_translated_post": False,
    "responsive_web_grok_analysis_button_from_backend": True,
    "post_ctas_fetch_enabled": True,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "responsive_web_grok_image_annotation_enabled": True,
    "responsive_web_grok_imagine_annotation_enabled": True,
    "responsive_web_grok_community_note_auto_translation_is_enabled": False,
    "responsive_web_enhance_cards_enabled": False,
}


# Default Nitter instance pool. Only proven-working mirrors. Other candidates
# are listed in NITTER_CANDIDATE_HINTS below — verify with probe_nitter.py
# before adding to your config.json.
DEFAULT_NITTER_INSTANCES = [
    "https://nitter.tiekoetter.com",
]

NITTER_CANDIDATE_HINTS = [
    "https://xcancel.com",
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
    "https://nitter.privacyredirect.com",
    "https://lightbrd.com",
    "https://nitter.space",
    "https://nuku.trabun.org",
    "https://nitter.catsarch.com",
    "https://nitter.us.catsarch.com",
    "https://nitter.kareem.one",
    "https://nitter.kavin.rocks",
]


DEFAULT_CONFIG = {
    "since_date": "2015-01-01",
    "page_size": 20,
    "batch_size": 8,
    "request_interval_s": 16,
    "request_interval_min_s": 12,
    "request_interval_max_s": 22,
    "auth_fail_cooldown_s": 900,
    "account_recovery_enabled": True,
    "account_recovery_fast_first_attempt": True,
    "account_recovery_cooldown_s": 900,
    "account_recovery_max_cooldown_s": 3600,
    "account_recovery_init_attempts": 2,
    "account_health_max": 3,
    "wallet_retry_attempts": 3,
    # Init is the X bootstrap step that prepares x-client-transaction-id.
    # It must pass for API speed. These settings do not reduce runtime speed;
    # they only prevent all accounts from stampeding x.com at startup.
    "startup_init_all_accounts": True,
    "startup_init_min_parallel": 18,
    "startup_init_attempts": 1,
    "init_max_parallel": 18,
    "init_attempts": 3,
    "init_timeout_s": 35,
    "init_timeout_cap_s": 35,
    "init_request_timeout_s": 12,
    "init_request_timeout_cap_s": 12,
    "init_debug_body_chars": 250,
    "red_total_min": 10,
    "red_ratio_max": 0.3,
    "green_ratio_min": 0.5,
    "dump_state_every_s": 30,
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "telegram_report_interval_s": 1800,
    "telegram_report_min_interval_s": 60,
    "telegram_force_5min_reports": False,
    "telegram_target_per_5min": 2200,
    "telegram_batch_export_enabled": True,
    "telegram_batch_export_size": 20000,
    "telegram_batch_export_dir": "output/tg_batches",
    "telegram_batch_export_state": "output/tg_export_state.json",
    "telegram_batch_export_interval_s": 30,
    "telegram_pin_start": False,
    "telegram_pin_batches": True,
    "telegram_pin_reports": False,
    "telegram_allowed_user_ids": [],
    "telegram_pin_messages": False,
    "telegram_pin_disable_notification": True,
    "strict_wallet_validation": True,
    "default_wallet_balance": "100",
    "producer_progress_every_lines": 20000,
    "consumer_wait_log_s": 15,
    "engine_batch_wall_timeout_s": 120,
    "all_workers_dead_grace_s": 300,
    "log_first_batches_per_engine": 5,
    # ---- Nitter pool ----
    "nitter_enabled": True,
    "nitter_instances": DEFAULT_NITTER_INSTANCES,
    "nitter_url": "https://nitter.tiekoetter.com",  # legacy single-instance fallback
    "nitter_use_proxy": True,
    # 0.5s = 2 req/s/instance. With pool of 2-3 stable instances, this gives
    # 4-6 req/s combined Nitter throughput without hammering any one host.
    "nitter_per_instance_min_gap_s": 0.5,
    "nitter_per_worker_min_gap_s": 1.5,
    "nitter_instance_fail_threshold": 5,
    "nitter_429_fail_threshold": 1,
    "nitter_429_cooldown_s": 900,
    "nitter_instance_cooldown_base_s": 60,
    "nitter_instance_cooldown_max_s": 1800,
    "nitter_anubis_max_wall_s": 25,
    "nitter_request_timeout_s": 20,
    # Legacy
    "nitter_fail_threshold": 5,
    "nitter_cold_cooldown_s": 600,
    "nitter_min_interval_s": 2,
    # ---- batched search (THE big speed unlock) ----
    # One request handles up to N wallets via "WALLET1" OR "WALLET2" syntax.
    # Same rate-limit cost (1 unit) but processes N wallets — effective
    # per-account throughput multiplied by N. Conservative defaults: rare
    # wallets pile up cleanly into one query, dense wallets fall back to
    # individual queries automatically (see batch quality fallback).
    "api_batch_size": 12,
    "nitter_batch_size": 8,
    "api_404_refresh_retries": 1,
    "api_batch_split_on_404": False,
    "api_404_circuit_threshold": 6,
    "api_404_circuit_cooldown_s": 600,
    "api_transient_retries": 3,
    # If a batched response returns >= page_size tweets AND some wallets in
    # the batch got 0 hits, those wallets are rerun individually (covers the
    # case where one high-mention wallet ate the entire page).
    "batch_quality_fallback": True,
}


# ---- state -----------------------------------------------------------------

def _retry_on_locked(func):
    """Wrap a State method so it retries on 'database is locked'."""
    def wrapper(self, *args, **kwargs):
        delays = (0.05, 0.1, 0.2, 0.4)
        last_err = None
        for d in delays:
            try:
                with self.lock:
                    return func(self, *args, **kwargs)
            except sqlite3.OperationalError as e:
                if "lock" not in str(e).lower():
                    raise
                last_err = e
                time.sleep(d)
        try:
            with self.lock:
                return func(self, *args, **kwargs)
        except sqlite3.OperationalError as e:
            raise RuntimeError(
                f"sqlite locked after {len(delays)} retries: {e} (last: {last_err})"
            ) from e
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__
    return wrapper


class State:
    def __init__(self, path):
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA cache_size=-20000")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS checked_wallets(
                address TEXT PRIMARY KEY,
                checked_at INTEGER NOT NULL,
                mentions_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS dedup_profiles(
                handle TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS counters(
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
        """)

    def count_checked(self) -> int:
        with self.lock:
            row = self.conn.execute("SELECT COUNT(*) FROM checked_wallets").fetchone()
        return int(row[0]) if row else 0

    def get_counter(self, key: str) -> int:
        with self.lock:
            row = self.conn.execute("SELECT value FROM counters WHERE key=?", (key,)).fetchone()
        return int(row[0]) if row else 0

    @_retry_on_locked
    def incr_counter(self, key: str, by: int = 1):
        self.conn.execute(
            "INSERT INTO counters(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=value+?",
            (key, by, by),
        )

    def is_wallet_checked(self, address: str) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM checked_wallets WHERE address=?", (address.lower(),)
            ).fetchone()
        return row is not None

    @_retry_on_locked
    def mark_wallet_checked(self, address: str, mentions_count: int):
        self.conn.execute(
            "INSERT OR REPLACE INTO checked_wallets(address, checked_at, mentions_count) VALUES(?,?,?)",
            (address.lower(), int(time.time()), mentions_count),
        )

    @_retry_on_locked
    def mark_wallets_checked_batch(self, items):
        if not items:
            return
        now = int(time.time())
        self.conn.executemany(
            "INSERT OR REPLACE INTO checked_wallets(address, checked_at, mentions_count) VALUES(?,?,?)",
            [(a.lower(), now, c) for a, c in items],
        )

    def load_dedup_profiles_from_file(self, path: Path):
        if not path.exists():
            return 0
        count = 0
        batch = []
        BATCH = 5000
        with self.lock:
            self.conn.execute("BEGIN")
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        h = _extract_handle(line)
                        if h:
                            batch.append((h,))
                            count += 1
                            if len(batch) >= BATCH:
                                self.conn.executemany(
                                    "INSERT OR IGNORE INTO dedup_profiles(handle) VALUES(?)",
                                    batch,
                                )
                                batch.clear()
                                if count % 20000 == 0:
                                    log.info(f"  [dedup] loaded {count} profiles...")
                if batch:
                    self.conn.executemany(
                        "INSERT OR IGNORE INTO dedup_profiles(handle) VALUES(?)", batch
                    )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return count

    def checked_wallets_set(self) -> set:
        with self.lock:
            rows = self.conn.execute("SELECT address FROM checked_wallets").fetchall()
        return set(r[0] for r in rows)

    def filter_checked_subset(self, addresses):
        """Return subset of `addresses` (lowercased) that already exist in
        checked_wallets. Batched IN-query — uses index, no full table scan,
        no big set in RAM. Caller passes ~1000 at a time."""
        if not addresses:
            return set()
        # SQLite default limit is 999 placeholders; chunk if needed.
        result = set()
        CHUNK = 900
        with self.lock:
            for i in range(0, len(addresses), CHUNK):
                chunk = addresses[i:i + CHUNK]
                placeholders = ",".join("?" * len(chunk))
                rows = self.conn.execute(
                    f"SELECT address FROM checked_wallets WHERE address IN ({placeholders})",
                    chunk,
                ).fetchall()
                result.update(r[0] for r in rows)
        return result

    def wal_checkpoint(self):
        """Force WAL → main DB checkpoint. Reduces .wal file size and
        ensures durability of recent commits even under hard crash."""
        try:
            with self.lock:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            return True
        except Exception:
            return False

    def is_profile_deduped(self, handle: str) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM dedup_profiles WHERE handle=?", (handle.lower(),)
            ).fetchone()
        return row is not None

    @_retry_on_locked
    def add_profile(self, handle: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO dedup_profiles(handle) VALUES(?)", (handle.lower(),)
        )


def _extract_handle(url_or_handle: str):
    s = (url_or_handle or "").strip().rstrip("/")
    if not s or s.startswith("#"):
        return None
    if s.startswith("http"):
        try:
            s = urlparse(s).path.strip("/").split("/")[0]
        except Exception:
            return None
    return s.lstrip("@").lower() or None


# ---- classification --------------------------------------------------------

def classify(primary: int, total: int, cfg: dict) -> str:
    if total <= 0:
        return "NONE"
    if total == 1:
        return "YELLOW"
    ratio = primary / total
    red_total = cfg.get("red_total_min", 10)
    red_ratio = cfg.get("red_ratio_max", 0.3)
    green_ratio = cfg.get("green_ratio_min", 0.5)
    if total >= red_total and ratio < red_ratio:
        return "RED"
    if ratio > green_ratio:
        return "GREEN"
    return "YELLOW"


def format_result_line(wallet: str, primary_author: str, primary_count: int, total: int,
                       balance: str, color: str) -> str:
    if total == 1:
        mention_str = "(1 упоминание)"
    else:
        mention_str = f"({primary_count} из {total} упом.)"
    bal = balance if balance else "-"
    marker = " — ПРОВЕРКА ВРУЧНУЮ" if color in ("YELLOW", "RED") else ""
    return f"{wallet} https://x.com/{primary_author} {mention_str} {bal} {color}{marker}\n"


# ---- tweet parsing ---------------------------------------------------------

def parse_tweets(data):
    tweets = []
    try:
        instructions = (
            data.get("data", {})
            .get("search_by_raw_query", {})
            .get("search_timeline", {})
            .get("timeline", {})
            .get("instructions", [])
        )
    except AttributeError:
        return tweets

    for instr in instructions:
        if not isinstance(instr, dict):
            continue
        entries = list(instr.get("entries") or [])
        entry_obj = instr.get("entry")
        if isinstance(entry_obj, dict):
            entries.append(entry_obj)
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            eid = str(entry.get("entryId") or "")
            if not eid.startswith("tweet-"):
                continue
            content = entry.get("content") or {}
            item = content.get("itemContent") or {}
            result = (item.get("tweet_results") or {}).get("result") or {}
            if "tweet" in result and isinstance(result["tweet"], dict):
                result = result["tweet"]
            legacy = result.get("legacy") or {}
            user_legacy = (
                (result.get("core") or {})
                .get("user_results", {})
                .get("result", {})
                .get("legacy", {})
            ) or {}
            user_core = (
                (result.get("core") or {})
                .get("user_results", {})
                .get("result", {})
                .get("core", {})
            ) or {}
            screen = user_legacy.get("screen_name") or user_core.get("screen_name")
            tid = legacy.get("id_str") or result.get("rest_id") or eid.replace("tweet-", "")
            content = legacy.get("full_text") or ""
            if screen:
                tweets.append({"id": str(tid), "user": screen, "content": content})
    return tweets


_NITTER_USER_PATTERNS = [
    re.compile(r'<a class="username"[^>]*>@([A-Za-z0-9_]{1,15})</a>'),
    re.compile(r'class="username"[^>]*>@([A-Za-z0-9_]{1,15})</a>'),
]


def parse_nitter_tweets(html: str):
    """Parse Nitter HTML into [{user, content}, ...]. bs4 first (robust),
    regex as a fallback. Content is needed for per-wallet attribution in
    batched queries."""
    tweets = []
    try:
        soup = bs4.BeautifulSoup(html, "html.parser")
        for item in soup.select(".timeline-item, .tweet"):
            u_el = item.select_one(".username")
            if not u_el:
                continue
            c_el = item.select_one(".tweet-content")
            username = u_el.get_text(strip=True).lstrip("@")
            content = c_el.get_text(separator=" ", strip=True) if c_el else ""
            if username:
                tweets.append({"id": "", "user": username, "content": content})
    except Exception:
        pass
    if not tweets:
        # Regex fallback — usernames only, no content (batch mode degraded)
        for pat in _NITTER_USER_PATTERNS:
            m = pat.findall(html)
            if m:
                tweets = [{"id": "", "user": u, "content": ""} for u in m]
                break
    return tweets


def parse_nitter_users(html: str):
    """Backward-compat wrapper: returns list of usernames only."""
    return [t["user"] for t in parse_nitter_tweets(html)]


_WALLET_PATTERNS = [
    re.compile(r"^0x[a-fA-F0-9]{40}$"),
    re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$"),
    re.compile(r"^(bc1|tb1)[ac-hj-np-z02-9]{11,71}$", re.IGNORECASE),
    re.compile(r"^[13][a-km-zA-HJ-NP-Z1-9]{25,34}$"),
]


def is_valid_wallet_candidate(wallet: str) -> bool:
    """Accept common address formats, reject garbage lines before querying X."""
    w = (wallet or "").strip()
    return any(p.match(w) for p in _WALLET_PATTERNS)


def _wallet_casefold(wallet: str) -> bool:
    w = (wallet or "").lower()
    return w.startswith(("0x", "bc1", "tb1"))


def tweet_contains_wallet(text: str, wallet: str) -> bool:
    """True only when the tweet text contains the actual address token.

    Plain substring matching can count a wallet embedded inside a longer
    alphanumeric token. Contentless parser fallbacks are handled by callers and
    are never treated as evidence.
    """
    if not text or not wallet:
        return False

    if _wallet_casefold(wallet):
        haystack = text.lower()
        needle = wallet.lower()
    else:
        haystack = text
        needle = wallet

    start = 0
    while True:
        idx = haystack.find(needle, start)
        if idx < 0:
            return False
        before = haystack[idx - 1] if idx > 0 else ""
        after_idx = idx + len(needle)
        after = haystack[after_idx] if after_idx < len(haystack) else ""
        if (not before or not before.isalnum()) and (not after or not after.isalnum()):
            return True
        start = idx + 1


def filter_tweets_for_wallet(tweets, wallet: str):
    return [
        t for t in tweets
        if tweet_contains_wallet((t.get("content") or ""), wallet)
    ]


def partition_tweets_by_wallet(tweets, wallets):
    """For each wallet, keep only tweets whose text actually contains it.

    Contentless parser-fallback tweets must NOT be attributed to a wallet.
    If X/Nitter starts returning broad search results or the HTML parser loses
    tweet text, saving nothing is much safer than saving random posts.
    """
    result = {w: [] for w in wallets}
    for t in tweets:
        text = t.get("content") or ""
        if not text:
            continue
        for w in wallets:
            if tweet_contains_wallet(text, w):
                result[w].append(t)
    return result


# ---- Nitter pool -----------------------------------------------------------

class NitterInstance:
    """Mutable per-instance health state. Shared between all workers via the
    NitterPool — that's how the global circuit breaker knows that one worker's
    429 means the next worker should skip this host."""
    __slots__ = ("url", "host", "fail_streak", "cold_until", "last_used",
                 "cooldown_level", "total_success", "total_fail",
                 "last_failure_reason", "anubis_lock")

    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.host = urlparse(self.url).hostname or self.url
        self.fail_streak = 0
        self.cold_until = 0.0
        self.last_used = 0.0
        self.cooldown_level = 0
        self.total_success = 0
        self.total_fail = 0
        self.last_failure_reason = ""
        self.anubis_lock = asyncio.Lock()


class NitterPool:
    """Thread-safe (asyncio) round-robin pool of Nitter instances.
    Picks the healthy instance with the oldest last_used and waits for the
    per-instance min_gap if needed. Pool-level fail tracking — 18 workers
    don't each need to fail 5x before agreeing an instance is dead.
    Critical: when an instance is already cold, in-flight 429 stragglers do
    NOT escalate the cooldown level (else 5 in-flight failures could
    multiply 60s → 960s in a heartbeat)."""

    def __init__(self, instances_urls, cfg):
        urls = []
        seen = set()
        for u in instances_urls:
            u = (u or "").strip().rstrip("/")
            if u and u not in seen:
                seen.add(u)
                urls.append(u)
        if not urls:
            legacy = (cfg.get("nitter_url") or "").strip().rstrip("/")
            if legacy:
                urls = [legacy]
        self.instances = [NitterInstance(u) for u in urls]
        self.min_gap = float(cfg.get("nitter_per_instance_min_gap_s", 0.5))
        self.fail_threshold = int(cfg.get("nitter_instance_fail_threshold", 5))
        self.rate_fail_threshold = int(cfg.get("nitter_429_fail_threshold", 1))
        self.rate_cd = float(cfg.get("nitter_429_cooldown_s", 900))
        self.base_cd = float(cfg.get("nitter_instance_cooldown_base_s", 60))
        self.max_cd = float(cfg.get("nitter_instance_cooldown_max_s", 1800))
        self._lock = asyncio.Lock()
        log.info(f"[pool] initialized with {len(self.instances)} instance(s): "
                 f"{', '.join(i.host for i in self.instances)}")

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            healthy = [i for i in self.instances if now >= i.cold_until]
            if not healthy:
                return None, 0.0
            healthy.sort(key=lambda i: i.last_used)
            chosen = healthy[0]
            wait_s = max(0.0, (chosen.last_used + self.min_gap) - now)
            chosen.last_used = now + wait_s
        if wait_s > 0:
            await asyncio.sleep(wait_s)
        return chosen, wait_s

    def report_success(self, inst: NitterInstance):
        inst.fail_streak = 0
        inst.cooldown_level = 0
        inst.total_success += 1

    def report_failure(self, inst: NitterInstance, reason: str):
        inst.total_fail += 1
        inst.last_failure_reason = (reason or "")[:120]
        # Already cooling — ignore in-flight stragglers, don't escalate
        if time.monotonic() < inst.cold_until:
            return
        inst.fail_streak += 1
        is_rate_limited = "429" in inst.last_failure_reason or "rate-limited" in inst.last_failure_reason
        threshold = self.rate_fail_threshold if is_rate_limited else self.fail_threshold
        if inst.fail_streak >= threshold:
            cd = min(self.max_cd, self.base_cd * (2 ** inst.cooldown_level))
            if is_rate_limited:
                cd = min(self.max_cd, max(cd, self.rate_cd))
            inst.cold_until = time.monotonic() + cd
            inst.cooldown_level = min(inst.cooldown_level + 1, 8)
            inst.fail_streak = 0
            log.warning(f"[pool] {inst.host} OUT for {cd:.0f}s "
                        f"(level={inst.cooldown_level}, last={inst.last_failure_reason})")

    def healthy_count(self) -> int:
        now = time.monotonic()
        return sum(1 for i in self.instances if now >= i.cold_until)

    def summary(self) -> str:
        now = time.monotonic()
        parts = []
        for i in self.instances:
            cd_left = max(0.0, i.cold_until - now)
            tag = "HOT" if cd_left == 0 else f"COLD-{cd_left:.0f}s"
            parts.append(f"{i.host}={tag}(✓{i.total_success}/✗{i.total_fail})")
        return " | ".join(parts)


# ---- worker ----------------------------------------------------------------

class AccountFailed(Exception):
    pass


class Worker:
    def __init__(self, account: dict, state: State, cfg: dict,
                 out_queue: asyncio.Queue, db_queue: asyncio.Queue,
                 nitter_pool: NitterPool):
        self.label = account["label"]
        self.auth_token = account["auth_token"]
        self.ct0 = account["ct0"]
        self.proxy = account["proxy"]
        self.ua = _pick_ua(self.label)
        self.cookies = {"auth_token": self.auth_token, "ct0": self.ct0}
        self.state = state
        self.cfg = cfg
        self.out_queue = out_queue
        self.db_queue = db_queue
        self.pool = nitter_pool
        self.rate_remaining = 50
        self.rate_reset = 0
        self.health = cfg.get("account_health_max", 3)
        self.ct = None
        self.last_req_at_mono = 0.0
        self.last_nitter_call_mono = 0.0
        self.last_engine = "?"
        self.last_nitter_host = ""
        self.api_client: "httpx.AsyncClient | None" = None
        self._nitter_client = None
        self._nitter_kind = ""

    def _init_ct_sync(self):
        timeout = min(
            float(self.cfg.get("init_request_timeout_s", 12)),
            float(self.cfg.get("init_request_timeout_cap_s", 12)),
        )
        body_chars = int(self.cfg.get("init_debug_body_chars", 250))
        proxy_label = _mask_proxy(self.proxy)

        def snippet(text: str) -> str:
            return (text or "").replace("\n", " ").replace("\r", " ")[:body_chars]

        session = requests.Session()
        session.headers = generate_headers()
        session.headers["User-Agent"] = self.ua
        session.proxies = {"http": self.proxy, "https": self.proxy}
        session.cookies.update(self.cookies)

        t0 = time.time()
        try:
            log.info(f"[{self.label}] init stage home_get via {proxy_label} "
                     f"(timeout={timeout:.0f}s)")
            home = session.get("https://x.com", timeout=timeout, allow_redirects=True)
        except Exception as e:
            raise RuntimeError(
                f"stage=home_get proxy={proxy_label} "
                f"error={type(e).__name__}: {str(e)[:220]}"
            ) from e

        dt = time.time() - t0
        if home.status_code != 200:
            raise RuntimeError(
                f"stage=home_get proxy={proxy_label} status={home.status_code} "
                f"elapsed={dt:.1f}s final_url={home.url} body={snippet(home.text)!r}"
            )
        log.info(f"[{self.label}] init stage home_ok {dt:.1f}s")

        home_soup = bs4.BeautifulSoup(home.content, "html.parser")

        try:
            ondemand_url = get_ondemand_file_url(response=home_soup)
        except Exception as e:
            raise RuntimeError(
                f"stage=ondemand_url proxy={proxy_label} "
                f"error={type(e).__name__}: {str(e)[:220]} "
                f"home_body={snippet(home.text)!r}"
            ) from e
        if not ondemand_url:
            raise RuntimeError(
                f"stage=ondemand_url proxy={proxy_label} empty_url "
                f"home_body={snippet(home.text)!r}"
            )

        t1 = time.time()
        try:
            log.info(f"[{self.label}] init stage ondemand_get "
                     f"(timeout={timeout:.0f}s)")
            ondemand = session.get(ondemand_url, timeout=timeout, allow_redirects=True)
        except Exception as e:
            raise RuntimeError(
                f"stage=ondemand_get proxy={proxy_label} url={ondemand_url} "
                f"error={type(e).__name__}: {str(e)[:220]}"
            ) from e

        dt2 = time.time() - t1
        if ondemand.status_code != 200:
            raise RuntimeError(
                f"stage=ondemand_get proxy={proxy_label} status={ondemand.status_code} "
                f"elapsed={dt2:.1f}s url={ondemand_url} body={snippet(ondemand.text)!r}"
            )
        log.info(f"[{self.label}] init stage ondemand_ok {dt2:.1f}s")

        self.ct = ClientTransaction(
            home_page_response=home_soup,
            ondemand_file_response=ondemand.text,
        )
        for c in session.cookies:
            self.cookies[c.name] = c.value
        if self.cookies.get("ct0"):
            self.ct0 = self.cookies["ct0"]
        log.info(f"[{self.label}] init stage ct_ready")

    async def init(self, max_attempts: int = 3):
        loop = asyncio.get_running_loop()
        last_err = None
        ok = False
        timeout_s = min(
            float(self.cfg.get("init_timeout_s", 35)),
            float(self.cfg.get("init_timeout_cap_s", 35)),
        )
        for attempt in range(1, max_attempts + 1):
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(None, self._init_ct_sync),
                    timeout=timeout_s,
                )
                ok = True
                break
            except asyncio.TimeoutError as e:
                last_err = e
                backoff = 5 * (2 ** (attempt - 1))
                log.warning(f"[{self.label}] init attempt {attempt}/{max_attempts} TIMEOUT "
                            f"after {timeout_s:.0f}s (retry in {backoff}s)")
                if attempt < max_attempts:
                    await asyncio.sleep(backoff)
            except Exception as e:
                last_err = e
                backoff = 5 * (2 ** (attempt - 1))
                log.warning(f"[{self.label}] init attempt {attempt}/{max_attempts} failed: {e} "
                            f"(retry in {backoff}s)")
                if attempt < max_attempts:
                    await asyncio.sleep(backoff)
        if not ok:
            raise RuntimeError(f"init failed after {max_attempts} attempts: {last_err}")

        log.info(f"[{self.label}] init stage api_deferred")

    def _ensure_api_client(self):
        if self.api_client is not None:
            return self.api_client
        limits = httpx.Limits(
            max_keepalive_connections=4,
            max_connections=8,
            keepalive_expiry=60,
        )
        log.info(f"[{self.label}] api client create")
        self.api_client = httpx.AsyncClient(
            proxy=self.proxy,
            cookies=self.cookies,
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=limits,
            headers={"user-agent": self.ua},
        )
        log.info(f"[{self.label}] api client ready")
        return self.api_client

    async def close_api(self):
        if self.api_client is not None:
            try:
                await self.api_client.aclose()
            except Exception:
                pass
            self.api_client = None
        self.ct = None

    async def recover_api(self, max_attempts: int = 2):
        await self.close_api()
        self.rate_remaining = 50
        self.rate_reset = 0
        await self.init(max_attempts=max_attempts)
        self.health = int(self.cfg.get("account_health_max", 3))

    async def close(self):
        await self.close_api()
        if self._nitter_client is not None:
            try:
                if self._nitter_kind == "curl_cffi":
                    await self._nitter_client.close()
                else:
                    await self._nitter_client.aclose()
            except Exception:
                pass
            self._nitter_client = None

    async def _wait_rate(self):
        i_min = float(self.cfg.get("request_interval_min_s", 12))
        i_norm = float(self.cfg.get("request_interval_s", 16))
        i_max = float(self.cfg.get("request_interval_max_s", 22))

        if self.rate_remaining > 30:
            target = i_min
        elif self.rate_remaining > 10:
            target = i_norm
        else:
            target = i_max

        jitter = random.uniform(-0.5, 0.5)
        effective = max(8.0, target + jitter)

        now = time.monotonic()
        if self.last_req_at_mono:
            elapsed = now - self.last_req_at_mono
            if elapsed < effective:
                await asyncio.sleep(effective - elapsed)

        if self.rate_remaining < 3 and self.rate_reset:
            wait_s = self.rate_reset - time.time() + 2
            if wait_s > 0:
                log.info(f"[{self.label}] rate-wait {wait_s:.0f}s (remaining={self.rate_remaining})")
                await asyncio.sleep(wait_s)
                self.rate_remaining = 50

    def _ensure_nitter_client(self):
        if self._nitter_client is not None:
            return self._nitter_client
        timeout = float(self.cfg.get("nitter_request_timeout_s", 20))
        if _HAS_CURL_CFFI:
            kwargs = {
                "impersonate": "chrome124",
                "timeout": timeout,
                "allow_redirects": True,
            }
            if self.cfg.get("nitter_use_proxy", False):
                kwargs["proxy"] = self.proxy
            self._nitter_client = cc_requests.AsyncSession(**kwargs)
            self._nitter_kind = "curl_cffi"
        else:
            kwargs = {
                "timeout": timeout,
                "follow_redirects": True,
                "headers": {"User-Agent": self.ua},
            }
            if self.cfg.get("nitter_use_proxy", False):
                kwargs["proxy"] = self.proxy
            self._nitter_client = httpx.AsyncClient(**kwargs)
            self._nitter_kind = "httpx"
        return self._nitter_client

    async def _nitter_get(self, url: str, params: dict = None):
        client = self._ensure_nitter_client()
        if self._nitter_kind == "curl_cffi":
            return await client.get(
                url, params=params,
                timeout=float(self.cfg.get("nitter_request_timeout_s", 20)),
            )
        return await client.get(url, params=params)

    async def _anubis_solve(self, challenge_html: str, target_url: str,
                            inst: NitterInstance) -> bool:
        m = re.search(
            r'<script id="anubis_challenge" type="application/json">([\s\S]+?)</script>',
            challenge_html,
        )
        if not m:
            return False
        try:
            data = json.loads(m.group(1))
            challenge = data["challenge"]
            rules = data.get("rules", {})
            random_data = challenge["randomData"]
            difficulty = int(rules.get("difficulty") or challenge.get("difficulty", 4))
            challenge_id = challenge["id"]
        except Exception:
            return False

        prefix = "0" * difficulty
        max_iter = 50_000_000
        max_wall_s = float(self.cfg.get("nitter_anubis_max_wall_s", 25))
        loop = asyncio.get_running_loop()
        t0 = time.time()

        def _solve():
            n = 0
            deadline = time.time() + max_wall_s
            while n < max_iter:
                h = hashlib.sha256(f"{random_data}{n}".encode()).hexdigest()
                if h.startswith(prefix):
                    return n, h
                n += 1
                if n % 50_000 == 0 and time.time() > deadline:
                    return None, None
            return None, None

        nonce, hash_hex = await loop.run_in_executor(None, _solve)
        if nonce is None:
            log.warning(f"[{self.label}] anubis PoW timeout on {inst.host} "
                        f"(difficulty={difficulty}, >{max_wall_s:.0f}s)")
            return False
        elapsed_ms = int((time.time() - t0) * 1000)

        pass_url = f"{inst.url}/.within.website/x/cmd/anubis/api/pass-challenge"
        params = {
            "id": challenge_id,
            "response": hash_hex,
            "nonce": str(nonce),
            "redir": target_url,
            "elapsedTime": str(elapsed_ms),
        }
        try:
            r = await self._nitter_get(pass_url, params=params)
        except Exception:
            return False
        return "anubis_challenge" not in r.text

    async def _nitter_run_query(self, raw_query: str):
        """Core Nitter search. Accepts arbitrary query string (single wallet
        or 'WALLET1 OR WALLET2 OR ...' batched). Returns list of tweets with
        content, raises on failure."""
        worker_gap = float(self.cfg.get("nitter_per_worker_min_gap_s", 1.5))
        if self.last_nitter_call_mono:
            since = time.monotonic() - self.last_nitter_call_mono
            if since < worker_gap:
                await asyncio.sleep(worker_gap - since)

        inst, _ = await self.pool.acquire()
        if inst is None:
            raise RuntimeError("nitter pool: no healthy instance available")
        self.last_nitter_host = inst.host
        self.last_nitter_call_mono = time.monotonic()

        target_url = f"{inst.url}/search?f=tweets&q={quote(raw_query)}"

        try:
            r = await self._nitter_get(target_url)
        except Exception as e:
            self.pool.report_failure(inst, f"{type(e).__name__}: {str(e)[:80]}")
            raise RuntimeError(f"{inst.host}: {type(e).__name__}: {str(e)[:80]}") from e

        body = r.text or ""
        sc = r.status_code

        if sc == 429:
            self.pool.report_failure(inst, "http 429")
            raise RuntimeError(f"{inst.host} http 429")
        if sc in (502, 503, 504, 521, 522, 523, 524, 525):
            self.pool.report_failure(inst, f"http {sc}")
            raise RuntimeError(f"{inst.host} http {sc}")

        if "anubis_challenge" in body:
            async with inst.anubis_lock:
                r2 = await self._nitter_get(target_url)
                if "anubis_challenge" in (r2.text or ""):
                    ok = await self._anubis_solve(r2.text, target_url, inst)
                    if not ok:
                        self.pool.report_failure(inst, "anubis solve failed")
                        raise RuntimeError(f"{inst.host} anubis solve failed")
                    r = await self._nitter_get(target_url)
                else:
                    r = r2
                body = r.text or ""
                sc = r.status_code

        if sc != 200:
            self.pool.report_failure(inst, f"http {sc}")
            raise RuntimeError(f"{inst.host} http {sc}")
        if len(body) < 500:
            self.pool.report_failure(inst, f"tiny body {len(body)}b")
            raise RuntimeError(f"{inst.host} tiny body {len(body)}b")
        if "anubis_challenge" in body:
            self.pool.report_failure(inst, "anubis loop")
            raise RuntimeError(f"{inst.host} anubis loop")
        if "Instance has been rate limited" in body:
            self.pool.report_failure(inst, "instance rate-limited")
            raise RuntimeError(f"{inst.host} instance rate-limited")

        tweets = parse_nitter_tweets(body)
        self.pool.report_success(inst)
        return tweets

    async def _nitter_search(self, wallet: str):
        return await self._nitter_run_query(f'"{wallet}"')

    async def _nitter_search_batch(self, wallets: list):
        """Search up to N wallets in a single Nitter request. Same per-instance
        rate cost as single — N× throughput when wallets are sparse."""
        if len(wallets) == 1:
            tweets = await self._nitter_search(wallets[0])
            return partition_tweets_by_wallet(tweets, wallets)
        or_query = " OR ".join(f'"{w}"' for w in wallets)
        tweets = await self._nitter_run_query(or_query)
        result = partition_tweets_by_wallet(tweets, wallets)
        # Quality fallback: if response saturated AND some wallets show 0,
        # they were likely starved by a high-mention wallet → individual retry.
        if (self.cfg.get("batch_quality_fallback", True)
                and len(tweets) >= 18  # near page-size saturation
                and any(not v for v in result.values())):
            for w in [w for w in wallets if not result[w]]:
                try:
                    result[w] = filter_tweets_for_wallet(await self._nitter_search(w), w)
                except Exception:
                    pass  # leave empty, will be marked 0 mentions
        return result

    async def _api_run_query(self, raw_query: str):
        """Core SearchTimeline call with arbitrary query (single or batched).
        Returns list of tweets with content. Raises on auth/rate/etc."""
        if self.ct is None:
            raise AccountFailed(f"{self.label}: API is not initialized")
        api_client = self._ensure_api_client()
        max_rate_retries = 10
        rate_retries = 0
        max_transient_retries = int(self.cfg.get("api_transient_retries", 3))
        transient_retries = 0
        max_404_retries = int(self.cfg.get("api_404_refresh_retries", 1))
        api_404_retries = 0
        while True:
            await self._wait_rate()
            variables = {
                "rawQuery": raw_query,
                "count": int(self.cfg.get("page_size", 20)),
                "querySource": "typed_query",
                "product": "Latest",
                "withGrokTranslatedBio": False,
            }
            # X migrated SearchTimeline from GET query parameters to POST JSON body.
            # Keeping GET causes global empty-body 404 with valid cookies/proxies.
            body = {
                "variables": variables,
                "features": FEATURES,
                "queryId": QUERY_ID,
            }
            tx_id = self.ct.generate_transaction_id(method="POST", path=urlparse(SEARCH_URL).path)
            headers = {
                "authorization": BEARER,
                "x-csrf-token": self.ct0,
                "x-twitter-active-user": "yes",
                "x-twitter-auth-type": "OAuth2Session",
                "x-twitter-client-language": "en",
                "x-client-transaction-id": tx_id,
                "user-agent": self.ua,
                "accept": "*/*",
                "content-type": "application/json",
                "accept-language": "en-US,en;q=0.9",
                "referer": "https://x.com/search?q=ethereum&src=typed_query&f=live",
            }
            self.last_req_at_mono = time.monotonic()
            r = await api_client.post(SEARCH_URL, json=body, headers=headers)

            rem = r.headers.get("x-rate-limit-remaining")
            rst = r.headers.get("x-rate-limit-reset")
            if rem is not None:
                try:
                    self.rate_remaining = int(rem)
                except ValueError:
                    pass
            if rst is not None:
                try:
                    self.rate_reset = int(rst)
                except ValueError:
                    pass

            if r.status_code == 200:
                try:
                    payload = r.json()
                except Exception as je:
                    raise RuntimeError(
                        f"status=200 but body not JSON: {type(je).__name__}: {je}; "
                        f"body_start={r.text[:200]!r}"
                    )
                if isinstance(payload, dict) and payload.get("errors"):
                    errs = payload.get("errors")
                    err_repr = json.dumps(errs, ensure_ascii=False)[:300]
                    is_rate = False
                    is_transient = False
                    for err in errs:
                        msg = str((err or {}).get("message", "")).lower()
                        code = str((err or {}).get("code", "")).lower()
                        name = str(((err or {}).get("extensions") or {}).get("name", "")).lower()
                        kind = str(((err or {}).get("extensions") or {}).get("kind", "")).lower()
                        if "rate" in msg or code == "88":
                            is_rate = True
                            break
                        if (code == "29" or "timeout" in msg or "serviceunavail" in msg
                                or "internalservererror" in name or "timeout" in name
                                or kind in ("operational", "servicelevel")):
                            is_transient = True
                            continue
                        if "auth" in msg or "unauthorized" in msg or code in ("32", "89"):
                            self.health -= 1
                            raise AccountFailed(
                                f"{self.label}: graphql auth-error {err_repr}, "
                                f"health={self.health}"
                            )
                    if is_rate:
                        rate_retries += 1
                        if rate_retries > max_rate_retries:
                            raise RuntimeError(
                                f"{self.label}: graphql rate-error after "
                                f"{max_rate_retries} retries: {err_repr}"
                            )
                        await asyncio.sleep(5)
                        continue
                    if is_transient:
                        transient_retries += 1
                        if transient_retries <= max_transient_retries:
                            wait_s = min(30, 3 * transient_retries)
                            log.warning(
                                f"[{self.label}] graphql transient error, wait {wait_s}s "
                                f"(retry {transient_retries}/{max_transient_retries}): "
                                f"{err_repr}"
                            )
                            await asyncio.sleep(wait_s)
                            continue
                    raise RuntimeError(f"{self.label}: graphql errors: {err_repr}")
                api_circuit_note_success()
                return parse_tweets(payload)

            if r.status_code == 429:
                rate_retries += 1
                if rate_retries > max_rate_retries:
                    raise RuntimeError(
                        f"{self.label}: 429 after {max_rate_retries} retries"
                    )
                wait_s = max(5, self.rate_reset - time.time() + 5)
                log.warning(f"[{self.label}] unexpected 429, wait {wait_s:.0f}s "
                            f"(retry {rate_retries}/{max_rate_retries})")
                await asyncio.sleep(wait_s)
                self.rate_remaining = 50
                continue
            if r.status_code in (401, 403):
                self.health -= 1
                raise AccountFailed(
                    f"{self.label}: auth {r.status_code}, health={self.health}, "
                    f"body={r.text[:200]!r}"
                )
            if r.status_code == 404:
                # Fast-fail 404 storms. In the provided log, SearchTimeline returns
                # empty-body 404 thousands of times; recursive batch splitting and
                # per-request refresh retries cut throughput roughly in half.
                # Retry only when we actually discover a NEW query_id. If the
                # refresh is "same", "recent", or "failed", immediately fall back
                # to the other engine instead of burning 8+ extra requests.
                if api_404_retries < max_404_retries:
                    api_404_retries += 1
                    status = await refresh_search_timeline_query_id(
                        self.proxy,
                        reason=f"api 404 from {self.label}",
                    )
                    if status == "updated":
                        log.warning(
                            f"[{self.label}] api 404 -> query_id refresh "
                            f"{status}, retry {api_404_retries}/{max_404_retries}"
                        )
                        continue
                    api_circuit_note_404(self.cfg, self.label, status)
                    raise RuntimeError(
                        f"{self.label}: api 404 transient/stale endpoint "
                        f"({status}); body={r.text[:160]!r}"
                    )
                api_circuit_note_404(self.cfg, self.label, "exhausted")
                raise RuntimeError(
                    f"{self.label}: api 404 after refresh; "
                    f"body={r.text[:160]!r}"
                )
            raise RuntimeError(
                f"{self.label}: status={r.status_code} body={r.text[:200]!r}"
            )

    async def _api_search(self, wallet: str):
        raw_query = f'"{wallet}" since:{self.cfg.get("since_date", "2015-01-01")}'
        return await self._api_run_query(raw_query)

    async def _api_search_batch(self, wallets: list):
        """Search up to N wallets in ONE Twitter request via OR. Same
        per-account rate cost (1 unit) — N× effective wallets/min."""
        if len(wallets) == 1:
            tweets = await self._api_search(wallets[0])
            return partition_tweets_by_wallet(tweets, wallets)
        or_query = " OR ".join(f'"{w}"' for w in wallets)
        raw_query = f'{or_query} since:{self.cfg.get("since_date", "2015-01-01")}'
        try:
            tweets = await self._api_run_query(raw_query)
        except Exception as e:
            msg = str(e).lower()
            if (self.cfg.get("api_batch_split_on_404", False)
                    and "api 404" in msg
                    and "transient/stale endpoint" not in msg):
                mid = max(1, len(wallets) // 2)
                log.warning(
                    f"[{self.label}] api batch×{len(wallets)} got 404; "
                    f"split into {mid}+{len(wallets) - mid}"
                )
                left = await self._api_search_batch(wallets[:mid])
                right = await self._api_search_batch(wallets[mid:])
                merged = dict(left)
                merged.update(right)
                return merged
            raise
        result = partition_tweets_by_wallet(tweets, wallets)
        page_size = int(self.cfg.get("page_size", 20))
        if (self.cfg.get("batch_quality_fallback", True)
                and len(tweets) >= page_size - 2
                and any(not v for v in result.values())):
            for w in [w for w in wallets if not result[w]]:
                try:
                    result[w] = filter_tweets_for_wallet(await self._api_search(w), w)
                except Exception:
                    pass
        return result

    async def fetch_via_nitter(self, wallet: str):
        return filter_tweets_for_wallet(await self._nitter_search(wallet), wallet)

    async def fetch_via_api(self, wallet: str):
        return filter_tweets_for_wallet(await self._api_search(wallet), wallet)

    async def fetch_via_nitter_batch(self, wallets: list):
        return await self._nitter_search_batch(wallets)

    async def fetch_via_api_batch(self, wallets: list):
        return await self._api_search_batch(wallets)

    def api_ready(self) -> bool:
        return self.ct is not None

    async def process_tweets(self, wallet: str, balance: str, tweets: list):
        tweets = filter_tweets_for_wallet(tweets, wallet)
        mentions_count = len(tweets)
        await self.db_queue.put(("mark", wallet, mentions_count))

        if mentions_count == 0:
            return "no-mentions", None
        authors = Counter(t["user"] for t in tweets if t.get("user"))
        if not authors:
            return "no-mentions", None
        primary, primary_count = authors.most_common(1)[0]
        total = mentions_count
        color = classify(primary_count, total, self.cfg)
        if await asyncio.to_thread(self.state.is_profile_deduped, primary):
            return "deduped", primary
        line = format_result_line(wallet, primary, primary_count, total, balance, color)
        await self.db_queue.put(("profile", primary))
        await self.out_queue.put((line, primary))
        return color, line


# ---- engine consumer (the heart of the dual-engine speedup) ----------------

def _safe_requeue(task_queue: asyncio.Queue, items: list, label: str = ""):
    """Non-blocking re-queue. If the bounded queue is full, items are dropped
    (they are NOT marked as checked, so the next run will pick them up).
    This prevents a deadlock where all consumers block on put() while the
    producer has already filled the freed slots."""
    requeued = 0
    dropped = 0
    for it in items:
        try:
            task_queue.put_nowait(it)
            requeued += 1
        except asyncio.QueueFull:
            dropped += 1
    if dropped:
        log.warning(
            f"[{label}] requeue: {requeued} ok, {dropped} dropped "
            f"(queue full at {task_queue.maxsize}; dropped wallets retry next run)"
        )


async def _engine_consumer(w: "Worker", task_queue: asyncio.Queue,
                           tg: "TelegramNotifier", active_workers: dict,
                           total_accs: int, engine: str):
    """Pull a BATCH of wallets and process them via one bulk request.
    'nitter' and 'api' run as TWO concurrent consumers per worker —
    combined throughput is the SUM of both. Batched search via the
    'WALLET1 OR WALLET2' Twitter query operator multiplies API/Nitter
    effective wallets-per-request by N, blowing past the per-account
    rate-limit ceiling without burning more rate-units.

    Strategy:
    - Nitter consumer parks PASSIVELY while pool is cold (no wallet bouncing
      into a 16M FIFO).
    - Each iteration pulls 1 wallet (blocking) then drains up to batch_size-1
      more from the queue without blocking, builds a single OR-query.
    - On rate-limit / transient errors: try the OTHER engine for the whole
      batch. If that also fails: log lightly, wallets stay in wallets.txt
      and are picked up on the next run.
    - Quality fallback: if Nitter/API page is saturated AND some wallets in
      the batch returned 0 tweets, those are re-queried individually."""
    cfg = w.cfg
    pool = w.pool
    label = w.label
    batch_size = int(cfg.get(f"{engine}_batch_size", 3))

    def _is_rate_limited(err) -> bool:
        s = str(err).lower()
        return "429" in s or "rate" in s or "no healthy" in s

    def _is_transient_net(err) -> bool:
        return type(err).__name__ in (
            "ReadError", "ConnectError", "ReadTimeout",
            "ConnectTimeout", "RemoteProtocolError", "PoolTimeout",
        )

    async def _do_batch_fetch(items: list, eng: str):
        """Single-attempt batched fetch with 1 retry on transient net errors.
        Returns dict {wallet: tweets} or raises last error.

        Anti-hang fix: every whole batch has a wall-clock timeout. If some
        network/client call gets stuck despite per-request timeouts, the worker
        logs it and the loop continues instead of looking frozen for 40 minutes.
        """
        wallets = [w_ for w_, _ in items]
        last_err_inner = None
        wall_timeout = max(30.0, float(cfg.get("engine_batch_wall_timeout_s", 120)))
        for attempt in (1, 2):
            try:
                if eng == "nitter":
                    return await asyncio.wait_for(w.fetch_via_nitter_batch(wallets), timeout=wall_timeout)
                return await asyncio.wait_for(w.fetch_via_api_batch(wallets), timeout=wall_timeout)
            except AccountFailed:
                raise
            except asyncio.TimeoutError as e:
                last_err_inner = RuntimeError(f"{eng} batch wall-timeout after {wall_timeout:.0f}s")
                break
            except Exception as e:
                last_err_inner = e
                if _is_rate_limited(e):
                    break
                if attempt == 1 and _is_transient_net(e):
                    await asyncio.sleep(2)
                    continue
                break
        raise last_err_inner

    async def _cooldown_or_shutdown(seconds: float) -> bool:
        """Sleep in small chunks so a recovering API worker can still exit fast.
        Does NOT consume/re-queue items — just sleeps and checks for sentinels
        by peeking at qsize. If a sentinel is already being processed by another
        consumer, it will naturally propagate."""
        deadline = time.monotonic() + max(0.0, seconds)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            await asyncio.sleep(min(5.0, remaining))

    async def _recover_api_loop(reason) -> bool:
        if not cfg.get("account_recovery_enabled", True):
            return False

        cooldown = float(cfg.get(
            "account_recovery_cooldown_s",
            cfg.get("auth_fail_cooldown_s", 900),
        ))
        cooldown = max(1.0, cooldown)
        max_cooldown = max(
            cooldown,
            float(cfg.get("account_recovery_max_cooldown_s", 3600)),
        )
        init_attempts = max(1, int(cfg.get(
            "account_recovery_init_attempts",
            cfg.get("init_attempts", 2),
        )))
        fast_first = bool(cfg.get("account_recovery_fast_first_attempt", True))
        first_attempt = True

        while True:
            if fast_first and first_attempt:
                log.warning(
                    f"[{label}] API fast recovery after account error: "
                    f"{str(reason)[:180]}"
                )
                first_attempt = False
            else:
                log.warning(
                    f"[{label}] API paused for {cooldown:.0f}s after account error: "
                    f"{str(reason)[:180]}"
                )
                tg.note("api_paused")
                if not await _cooldown_or_shutdown(cooldown):
                    return False
            try:
                log.info(f"[{label}] API recovery init start ...")
                await w.recover_api(max_attempts=init_attempts)
                active_workers[label] = w
                log.info(f"[{label}] API recovered, health={w.health}")
                tg.note("api_recovered")
                return True
            except Exception as e:
                log.warning(
                    f"[{label}] API recovery failed: {type(e).__name__}: "
                    f"{str(e)[:180]}"
                )
                cooldown = min(max_cooldown, cooldown * 2)

    while True:
        # If SearchTimeline is globally returning 404, do not let API workers
        # pull wallets and immediately fallback into Nitter. Park them until
        # the circuit cooldown expires; Nitter consumers keep working normally.
        if engine == "api":
            sleep_left = api_circuit_sleep_left()
            if sleep_left > 0:
                await asyncio.sleep(min(5.0, sleep_left))
                continue

        # Park nitter consumer passively while pool is cold.
        if engine == "nitter":
            while pool.healthy_count() == 0:
                await asyncio.sleep(0.5)

        # Pull 1 item. Timeout is only for diagnostics; it prevents silent waits.
        wait_log_s = max(5.0, float(cfg.get("consumer_wait_log_s", 15)))
        while True:
            try:
                first = await asyncio.wait_for(task_queue.get(), timeout=wait_log_s)
                break
            except asyncio.TimeoutError:
                log.info(f"[{label}/{engine}] waiting input... qsize={task_queue.qsize()}")
        if first is None:
            task_queue.task_done()
            return

        items = [first]
        sentinel_seen = False
        while len(items) < batch_size:
            try:
                item = task_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is None:
                # Sentinel mid-batch — done count it now, exit after this batch.
                task_queue.task_done()
                sentinel_seen = True
                break
            items.append(item)

        try:
            tweets_by_wallet = None
            last_err = None
            tried_other = False

            try:
                # visible heartbeat for the first few batches from every engine
                w.batch_debug_counter = getattr(w, "batch_debug_counter", 0) + 1
                if w.batch_debug_counter <= int(cfg.get("log_first_batches_per_engine", 5)):
                    log.info(f"[{label}/{engine}] processing batch×{len(items)} sample={items[0][0][:24]} qsize={task_queue.qsize()}")
                tweets_by_wallet = await _do_batch_fetch(items, engine)
                w.last_engine = (f"nitter:{w.last_nitter_host}" if engine == "nitter"
                                 else "api")
            except AccountFailed as e:
                log.warning(f"[{label}] AccountFailed ({engine}): {e}")
                _safe_requeue(task_queue, items, label)
                if engine == "api":
                    log.warning(f"[{label}] API will try to recover; Nitter stays alive")
                    if await _recover_api_loop(e):
                        continue
                    return
                if w.health <= 0:
                    log.warning(f"[{label}] retired (health=0)")
                    active_workers.pop(label, None)
                    tg.note("retired")
                    return
                await asyncio.sleep(cfg.get("auth_fail_cooldown_s", 900))
                return
            except Exception as e:
                last_err = e

            # Cross-engine fallback for the WHOLE batch
            if tweets_by_wallet is None and last_err is not None:
                tried_other = True
                try:
                    if engine == "nitter":
                        if w.api_ready() and api_circuit_sleep_left() <= 0:
                            tweets_by_wallet = await w.fetch_via_api_batch(
                                [w_ for w_, _ in items]
                            )
                            w.last_engine = "api(fb)"
                    else:
                        if pool.healthy_count() > 0:
                            tweets_by_wallet = await w.fetch_via_nitter_batch(
                                [w_ for w_, _ in items]
                            )
                            w.last_engine = f"nitter(fb):{w.last_nitter_host}"
                except AccountFailed as e:
                    log.warning(f"[{label}] AccountFailed on fallback: {e}")
                    _safe_requeue(task_queue, items, label)
                    if engine == "api":
                        if await _recover_api_loop(e):
                            continue
                        return
                    log.warning(f"[{label}] API fallback failed; Nitter keeps running")
                    continue
                except Exception as e:
                    last_err = e

            if tweets_by_wallet is not None:
                for wallet, balance in items:
                    tweets = tweets_by_wallet.get(wallet, [])
                    await w.process_tweets(wallet, balance, tweets)
            elif last_err is not None:
                if pool.healthy_count() == 0 and (not w.api_ready() or api_circuit_sleep_left() > 0):
                    _safe_requeue(task_queue, items, label)
                    sleep_s = min(
                        30.0,
                        max(5.0, api_circuit_sleep_left() if w.api_ready() else 5.0),
                    )
                    log.warning(
                        f"[{label}/{engine}] both engines unavailable; "
                        f"requeued batch×{len(items)} and sleep {sleep_s:.0f}s "
                        f"(last={type(last_err).__name__}: {str(last_err)[:140]})"
                    )
                    await asyncio.sleep(sleep_s)
                    continue
                msg = str(last_err) or repr(last_err)
                fb = " +fb" if tried_other else ""
                # Do not hide startup failures. After the first noisy phase, sample.
                err_count = getattr(w, "batch_error_counter", 0) + 1
                w.batch_error_counter = err_count
                if err_count <= 20 or random.random() < 0.05:
                    log.warning(f"[{label}/{engine}{fb}] batch×{len(items)} failed: "
                                f"{type(last_err).__name__}: {msg[:180]} "
                                f"(err#{err_count})")
        finally:
            for _ in items:
                task_queue.task_done()

        if sentinel_seen:
            return


# ---- worker loop -----------------------------------------------------------

async def worker_loop(acc: dict, state: State, cfg: dict,
                     task_queue: asyncio.Queue, out_queue: asyncio.Queue,
                     db_queue: asyncio.Queue,
                     active_workers: dict, tg: "TelegramNotifier",
                     total_accs: int, pool: NitterPool,
                     init_sem: asyncio.Semaphore = None):
    """One worker per account. Spawns two parallel sub-consumers — one for
    Nitter, one for direct Twitter — sharing the same Worker (and so the
    same proxy, auth, persistent HTTP clients)."""
    label = acc["label"]
    w = None
    api_ready = False
    try:
        w = Worker(acc, state, cfg, out_queue, db_queue, pool)
        startup_attempts = max(1, int(cfg.get(
            "startup_init_attempts",
            cfg.get("init_attempts", 1),
        )))
        log.info(f"[{label}] waiting init slot ...")
        if init_sem is None:
            log.info(f"[{label}] init start ...")
            await w.init(max_attempts=startup_attempts)
        else:
            async with init_sem:
                log.info(f"[{label}] init start ...")
                await w.init(max_attempts=startup_attempts)
        log.info(f"[{label}] ready")
        api_ready = True
        active_workers[label] = w
    except Exception as e:
        log.error(f"[{label}] init failed permanently: {e}\n{traceback.format_exc()}")
        tg.note("init_failed")
        if w is None or not cfg.get("nitter_enabled", True) or not pool.instances:
            if w is not None:
                await w.close()
            return
        try:
            w._ensure_nitter_client()
            active_workers[label] = w
            log.warning(f"[{label}] started in Nitter-only mode after API init failure")
            tg.note("nitter_only")
        except Exception as ne:
            log.error(f"[{label}] Nitter-only start failed: {ne}")
            await w.close()
            return

    try:
        async def _guarded_engine(engine_name: str):
            while True:
                try:
                    await _engine_consumer(
                        w, task_queue, tg, active_workers, total_accs, engine_name
                    )
                    return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.error(
                        f"[{label}/{engine_name}] consumer crashed; restart in 5s\n"
                        f"{traceback.format_exc()}"
                    )
                    tg.note("consumer_crashed")
                    await asyncio.sleep(5)

        tasks = [
            asyncio.create_task(_guarded_engine("nitter"), name=f"{label}-nitter")
        ]
        if api_ready:
            tasks.append(asyncio.create_task(
                _guarded_engine("api"),
                name=f"{label}-api",
            ))
        await asyncio.gather(*tasks)
    finally:
        active_workers.pop(label, None)
        if w is not None:
            await w.close()


# ---- async DB writer -------------------------------------------------------

async def db_writer(db_queue: asyncio.Queue, state: State, batch_max: int = 100,
                    flush_every_s: float = 0.5):
    """Single-writer coroutine. Coalesces mark_wallet_checked into batched
    transactions and runs every op in a thread to avoid blocking the loop."""
    pending_marks = []
    pending_profiles = []
    last_flush = time.monotonic()

    async def flush():
        nonlocal pending_marks, pending_profiles, last_flush
        if pending_marks:
            items = pending_marks
            pending_marks = []
            try:
                await asyncio.to_thread(state.mark_wallets_checked_batch, items)
            except Exception as e:
                log.error(f"[db] batch mark failed ({len(items)} items): {e}")
        if pending_profiles:
            for h in pending_profiles:
                try:
                    await asyncio.to_thread(state.add_profile, h)
                except Exception as e:
                    log.error(f"[db] add_profile {h!r} failed: {e}")
            pending_profiles = []
        last_flush = time.monotonic()

    while True:
        timeout = max(0.05, flush_every_s - (time.monotonic() - last_flush))
        try:
            item = await asyncio.wait_for(db_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            await flush()
            continue

        if item is None:
            db_queue.task_done()
            await flush()
            return

        try:
            kind = item[0]
            if kind == "mark":
                _, wallet, mentions = item
                pending_marks.append((wallet, mentions))
            elif kind == "profile":
                _, handle = item
                pending_profiles.append(handle)
            elif kind == "counter":
                _, key, by = item
                try:
                    await asyncio.to_thread(state.incr_counter, key, by)
                except Exception as e:
                    log.error(f"[db] incr_counter {key} failed: {e}")
        finally:
            db_queue.task_done()

        if (len(pending_marks) >= batch_max or
                len(pending_profiles) >= batch_max or
                time.monotonic() - last_flush >= flush_every_s):
            await flush()


# ---- output writer ---------------------------------------------------------

async def output_writer(queue: asyncio.Queue, db_queue: asyncio.Queue):
    OUTPUT_RESULTS.parent.mkdir(parents=True, exist_ok=True)
    INPUT_DEDUP.parent.mkdir(parents=True, exist_ok=True)
    f_results = open(OUTPUT_RESULTS, "a", encoding="utf-8", buffering=1)
    f_dedup = open(INPUT_DEDUP, "a", encoding="utf-8", buffering=1)
    try:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            try:
                line, primary = item
                f_results.write(line)
                f_dedup.write(f"https://x.com/{primary}\n")
                await db_queue.put(("counter", "results_written", 1))
                sys.stdout.write(line)
                sys.stdout.flush()
            except Exception as e:
                log.error(f"[writer] error: {e}")
            finally:
                queue.task_done()
    finally:
        try:
            f_results.close()
        except Exception:
            pass
        try:
            f_dedup.close()
        except Exception:
            pass


# ---- output stats ----------------------------------------------------------

def count_output_results() -> int:
    """Count actual non-empty result lines in output/results.txt.
    More reliable than the 'results_written' counter (which only tracks
    rows written by the current code path and can drift from reality if
    the file was edited manually or counter was reset)."""
    if not OUTPUT_RESULTS.exists():
        return 0
    n = 0
    try:
        with open(OUTPUT_RESULTS, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if line.strip():
                    n += 1
    except Exception as e:
        log.warning(f"[stats] count_output_results failed: {e}")
    return n


# ---- telegram --------------------------------------------------------------

class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, cfg: dict | None = None):
        self.token = (bot_token or "").strip()
        self.chat_id = (str(chat_id) if chat_id else "").strip()
        self.enabled = bool(self.token and self.chat_id)
        self.events = Counter()
        self.cfg = cfg or {}
        self.pin_enabled = bool(self.cfg.get("telegram_pin_messages", False))
        self.pin_disable_notification = bool(self.cfg.get("telegram_pin_disable_notification", True))

    async def pin_message(self, message_id: int) -> bool:
        """Pin a Telegram message if enabled. Failure is non-fatal."""
        if not self.enabled or not self.pin_enabled or not message_id:
            return False
        url = f"https://api.telegram.org/bot{self.token}/pinChatMessage"
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.post(url, json={
                    "chat_id": self.chat_id,
                    "message_id": int(message_id),
                    "disable_notification": self.pin_disable_notification,
                })
                if r.status_code != 200:
                    log.warning(f"[tg] pin failed: status={r.status_code} body={r.text[:200]}")
                    return False
                return True
        except Exception as e:
            log.warning(f"[tg] pin error: {e}")
            return False

    async def send(self, text: str, pin: bool = False, parse_mode: str = "HTML",
                   reply_markup: dict | None = None):
        if not self.enabled:
            return False
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                payload = {
                    "chat_id": self.chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                }
                if parse_mode:
                    payload["parse_mode"] = parse_mode
                if reply_markup:
                    payload["reply_markup"] = reply_markup
                r = await c.post(url, json=payload)
                if r.status_code != 200:
                    log.warning(f"[tg] send failed: status={r.status_code} body={r.text[:200]}")
                    return False
                message_id = None
                try:
                    message_id = (r.json().get("result") or {}).get("message_id")
                except Exception:
                    message_id = None
                if pin and message_id:
                    await self.pin_message(int(message_id))
                return message_id or True
        except Exception as e:
            log.warning(f"[tg] send error: {e}")
            return False

    async def edit_message(self, message_id: int, text: str,
                           parse_mode: str = "HTML",
                           reply_markup: dict | None = None) -> bool:
        if not self.enabled or not message_id:
            return False
        url = f"https://api.telegram.org/bot{self.token}/editMessageText"
        try:
            payload = {
                "chat_id": self.chat_id,
                "message_id": int(message_id),
                "text": text,
                "disable_web_page_preview": True,
            }
            if parse_mode:
                payload["parse_mode"] = parse_mode
            if reply_markup:
                payload["reply_markup"] = reply_markup
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.post(url, json=payload)
                return r.status_code == 200
        except Exception as e:
            log.warning(f"[tg] edit error: {e}")
            return False

    async def answer_callback(self, callback_id: str, text: str = "") -> bool:
        if not self.enabled:
            return False
        url = f"https://api.telegram.org/bot{self.token}/answerCallbackQuery"
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(url, json={
                    "callback_query_id": callback_id,
                    "text": text,
                })
                return r.status_code == 200
        except Exception:
            return False

    async def get_updates(self, offset: int = 0, timeout: int = 30) -> list:
        if not self.enabled:
            return []
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(timeout + 10, connect=10)) as c:
                r = await c.post(url, json={
                    "offset": offset,
                    "timeout": timeout,
                    "allowed_updates": ["message", "callback_query"],
                })
                if r.status_code == 200:
                    data = r.json()
                    result = data.get("result", [])
                    if result:
                        log.info(f"[tg] got {len(result)} update(s)")
                    return result
                else:
                    log.warning(f"[tg] getUpdates status={r.status_code} body={r.text[:200]}")
        except Exception as e:
            log.warning(f"[tg] getUpdates error: {e}")
        return []

    async def delete_message(self, message_id: int) -> bool:
        if not self.enabled or not message_id:
            return False
        url = f"https://api.telegram.org/bot{self.token}/deleteMessage"
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(url, json={
                    "chat_id": self.chat_id,
                    "message_id": int(message_id),
                })
                return r.status_code == 200
        except Exception:
            return False

    async def send_auto_delete(self, text: str, delay: float = 30.0,
                               parse_mode: str = "HTML",
                               reply_markup: dict | None = None):
        """Send a message that auto-deletes after `delay` seconds."""
        mid = await self.send(text, parse_mode=parse_mode, reply_markup=reply_markup)
        if mid and isinstance(mid, int):
            asyncio.get_event_loop().call_later(
                delay,
                lambda: asyncio.ensure_future(self.delete_message(mid)))
        return mid

    async def set_my_commands(self, commands: list[dict]) -> bool:
        if not self.enabled:
            return False
        url = f"https://api.telegram.org/bot{self.token}/setMyCommands"
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.post(url, json={"commands": commands})
                return r.status_code == 200
        except Exception:
            return False

    async def send_document(self, path: Path, caption: str = "", pin: bool = False,
                            parse_mode: str = "HTML") -> bool:
        if not self.enabled:
            return False
        url = f"https://api.telegram.org/bot{self.token}/sendDocument"
        try:
            async with httpx.AsyncClient(timeout=120) as c:
                with open(path, "rb") as f:
                    files = {"document": (path.name, f, "text/plain")}
                    data = {
                        "chat_id": self.chat_id,
                        "caption": caption[:1024],
                    }
                    if parse_mode:
                        data["parse_mode"] = parse_mode
                    r = await c.post(url, data=data, files=files)
                if r.status_code != 200:
                    log.warning(f"[tg] document failed: status={r.status_code} body={r.text[:300]}")
                    return False
                message_id = None
                try:
                    message_id = (r.json().get("result") or {}).get("message_id")
                except Exception:
                    message_id = None
                if pin and message_id:
                    await self.pin_message(int(message_id))
                return True
        except Exception as e:
            log.warning(f"[tg] document error for {path}: {e}")
            return False

    def note(self, event: str, amount: int = 1):
        self.events[event] += amount

    def drain_events(self) -> Counter:
        events = Counter(self.events)
        self.events.clear()
        return events


def _load_tg_batch_state(state_path: Path) -> dict:
    try:
        if state_path.exists():
            data = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception as e:
        log.warning(f"[tg-batch] failed to read state: {e}")
    return {"last_sent_batch": 0, "last_sent_line": 0, "sent_files": []}


def _save_tg_batch_state(state_path: Path, data: dict):
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(state_path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(state_path)


def _count_lines_fast(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            total += chunk.count(b"\n")
    return total


def _write_line_range(src: Path, dst: Path, start_line: int, end_line: int) -> int:
    """Write 1-based inclusive line range from src to dst. Returns written count."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    written = 0
    with open(src, "r", encoding="utf-8", errors="ignore") as inp, open(tmp, "w", encoding="utf-8", newline="") as out:
        for idx, line in enumerate(inp, start=1):
            if idx < start_line:
                continue
            if idx > end_line:
                break
            if line.strip():
                out.write(line if line.endswith("\n") else line + "\n")
                written += 1
    tmp.replace(dst)
    return written


async def telegram_batch_exporter(tg: TelegramNotifier, cfg: dict):
    """Send output/results.txt to Telegram in exact 20k batches, no duplicates."""
    if not tg.enabled:
        return
    if not cfg.get("telegram_batch_export_enabled", True):
        return

    batch_size = int(cfg.get("telegram_batch_export_size", 20000))
    interval = max(5, int(cfg.get("telegram_batch_export_interval_s", 30)))
    export_dir = BASE / str(cfg.get("telegram_batch_export_dir", "output/tg_batches"))
    state_path = BASE / str(cfg.get("telegram_batch_export_state", "output/tg_export_state.json"))
    export_dir.mkdir(parents=True, exist_ok=True)

    state_data = _load_tg_batch_state(state_path)
    last_sent_batch = int(state_data.get("last_sent_batch", 0) or 0)

    log.info(f"[tg-batch] enabled batch_size={batch_size} interval={interval}s last_sent_batch={last_sent_batch}")

    while True:
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return

        try:
            total_lines = await asyncio.to_thread(_count_lines_fast, OUTPUT_RESULTS)
            next_batch = last_sent_batch + 1
            next_end = next_batch * batch_size

            # Send all fully completed batches. Last incomplete chunk is not sent.
            while total_lines >= next_end:
                start_line = (next_batch - 1) * batch_size + 1
                end_line = next_end
                filename = f"{batch_size}({next_batch}).txt"
                out_path = export_dir / filename

                written = await asyncio.to_thread(_write_line_range, OUTPUT_RESULTS, out_path, start_line, end_line)
                if written != batch_size:
                    log.warning(f"[tg-batch] {filename}: expected {batch_size}, wrote {written}; will retry later")
                    try:
                        out_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    break

                w_n = f"{written:,}".replace(",", " ")
                s_n = f"{start_line:,}".replace(",", " ")
                e_n = f"{end_line:,}".replace(",", " ")
                t_n = f"{total_lines:,}".replace(",", " ")
                caption = (
                    f"<b>📦 Новый батч кошельков</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📄 Файл: <code>{_h(filename)}</code>\n"
                    f"✅ Строк: <b>{w_n}</b>\n"
                    f"🔢 Диапазон: <code>{s_n} – {e_n}</code>\n"
                    f"🎯 Всего: <b>{t_n}</b>\n\n"
                    f"🕒 <i>{time.strftime('%Y-%m-%d %H:%M:%S')}</i>"
                )

                ok = await tg.send_document(out_path, caption, pin=bool(cfg.get("telegram_pin_batches", True)))
                if not ok:
                    log.warning(f"[tg-batch] failed to send {filename}; will retry later")
                    break

                last_sent_batch = next_batch
                state_data = {
                    "last_sent_batch": last_sent_batch,
                    "last_sent_line": end_line,
                    "sent_files": list((state_data.get("sent_files") or [])[-50:]) + [filename],
                    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
                await asyncio.to_thread(_save_tg_batch_state, state_path, state_data)
                log.info(f"[tg-batch] sent {filename} lines {start_line}-{end_line}")

                next_batch = last_sent_batch + 1
                next_end = next_batch * batch_size

        except Exception as e:
            log.warning(f"[tg-batch] exporter error: {e}")


async def telegram_reporter(tg: TelegramNotifier, state: State,
                            task_queue: asyncio.Queue, active_workers: dict,
                            pool: NitterPool, total_accs: int, interval: int, cfg: dict | None = None):
    if not tg.enabled:
        return
    last_checked = state.count_checked()
    while True:
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return
        try:
            current_checked = state.count_checked()
            delta = current_checked - last_checked
            remaining = task_queue.qsize()
            total_results = state.get_counter("results_written")
            results_in_file = count_output_results()
            alive = len(active_workers)
            healthy_n = pool.healthy_count() if pool else 0
            total_n = len(pool.instances) if pool else 0
            events = tg.drain_events()
            event_lines = []
            if events.get("api_paused"):
                event_lines.append(f"⏸ API пауз/ошибок: {events['api_paused']}")
            if events.get("api_recovered"):
                event_lines.append(f"✅ API восстановилось: {events['api_recovered']}")
            if events.get("init_failed"):
                event_lines.append(f"⚠️ Init не прошел: {events['init_failed']}")
            if events.get("nitter_only"):
                event_lines.append(f"🟡 Nitter-only стартов: {events['nitter_only']}")
            if events.get("retired"):
                event_lines.append(f"🔴 Акков снято: {events['retired']}")
            if events.get("all_dead"):
                event_lines.append("🚨 Все воркеры остановились")
            if events.get("consumer_crashed"):
                event_lines.append(f"♻️ Воркер-циклов перезапущено: {events['consumer_crashed']}")
            if events.get("started"):
                event_lines.append("🚀 Парсер стартовал")
            if events.get("finished"):
                event_lines.append("🏁 Парсер завершился")
            events_text = "\n".join(event_lines) if event_lines else "🟢 Без критичных событий"
            minutes = max(1, int(interval / 60))
            per_5_min = int(delta * 300 / max(1, interval))
            target_5m = int(getattr(state, "cfg_target_per_5min", 2200) or 2200)
            target_tag = "✅ норма" if per_5_min >= target_5m else "⚠️ ниже цели"
            api_cd = api_circuit_sleep_left()
            api_status = "🟢 HOT" if api_cd <= 0 else f"🔴 COLD {api_cd:.0f}s"
            pct = int(min(100, per_5_min / max(1, target_5m) * 100))
            bar = "▓" * (pct // 10) + "░" * (10 - pct // 10)
            text = (
                f"<b>📊 Отчёт парсера</b> <i>({minutes} мин)</i>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<b>⚡ Скорость</b>\n"
                f"├ За период: <b>+{delta}</b>\n"
                f"├ Темп: <b>~{per_5_min}</b>/5мин (цель {target_5m}) {target_tag}\n"
                f"└ <code>[{bar}]</code> {pct}%\n\n"
                f"<b>📈 Прогресс</b>\n"
                f"├ 📦 Очередь: <b>{remaining}</b>\n"
                f"├ 💾 В базе: <b>{current_checked}</b>\n"
                f"├ 🎯 Результатов: <b>{results_in_file}</b>\n"
                f"└ 🧾 Записано: <b>{total_results}</b>\n\n"
                f"<b>⚙️ Инфра</b>\n"
                f"├ 🧠 API: {api_status}\n"
                f"├ 👥 Аккаунты: <b>{alive}/{total_accs}</b>\n"
                f"└ 🌐 Nitter: <b>{healthy_n}/{total_n}</b>\n\n"
                f"<b>📌 События:</b>\n{events_text}"
            )
            await tg.send(text, pin=bool((cfg or {}).get("telegram_pin_reports", False)))
            last_checked = current_checked
        except Exception as e:
            log.warning(f"[tg] reporter error: {e}")


# ---- main ------------------------------------------------------------------

def load_config():
    if CONFIG_FILE.exists():
        try:
            user = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            merged = {**DEFAULT_CONFIG, **user}
            legacy_aliases = (
                ("batch_size", "api_batch_size"),
                ("batch_size", "nitter_batch_size"),
                ("per_instance_min_gap_s", "nitter_per_instance_min_gap_s"),
                ("instance_fail_threshold", "nitter_instance_fail_threshold"),
                ("instance_cooldown_base_s", "nitter_instance_cooldown_base_s"),
                ("instance_cooldown_max_s", "nitter_instance_cooldown_max_s"),
                ("request_timeout_s", "nitter_request_timeout_s"),
            )
            for old_key, new_key in legacy_aliases:
                if old_key == "batch_size" and old_key in user:
                    # Old config used one speed knob. Keep it authoritative so
                    # stale generated api_batch_size=3 cannot silently slow runs.
                    merged[new_key] = user[old_key]
                elif old_key in user and new_key not in user:
                    merged[new_key] = user[old_key]
            if user.get("nitter_url"):
                u = user["nitter_url"].strip().rstrip("/")
                if u and u not in merged.get("nitter_instances", []):
                    merged["nitter_instances"] = [u] + list(merged.get("nitter_instances", []))
            return merged
        except Exception as e:
            log.warning(f"[config] failed to load, using defaults: {e}")
    CONFIG_FILE.write_text(
        json.dumps(DEFAULT_CONFIG, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return DEFAULT_CONFIG.copy()


def _normalize_proxy(raw: str) -> str:
    p = (raw or "").strip()
    if not p:
        return p
    if "://" in p:
        return p
    return "http://" + p


def _mask_proxy(proxy_url: str) -> str:
    """Hide proxy password in logs, keep host visible for diagnostics."""
    if not proxy_url:
        return "direct"
    try:
        u = urlparse(proxy_url)
        host = u.hostname or "?"
        port = f":{u.port}" if u.port else ""
        user = u.username or ""
        auth = f"{user}:***@" if user else ""
        return f"{u.scheme}://{auth}{host}{port}"
    except Exception:
        return "<bad-proxy-url>"


def load_accounts():
    if not ACCOUNTS_FILE.exists():
        log.error(f"{ACCOUNTS_FILE} not found")
        sys.exit(1)
    data = json.loads(ACCOUNTS_FILE.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("accounts", [])
    if not data:
        log.error("accounts.json is empty")
        sys.exit(1)
    fixed_proxies = 0
    for i, a in enumerate(data):
        if not all(k in a for k in ("label", "auth_token", "ct0", "proxy")):
            log.error(f"account #{i} missing keys. Required: label, auth_token, ct0, proxy")
            sys.exit(1)
        original = a["proxy"]
        normalized = _normalize_proxy(original)
        if normalized != original:
            a["proxy"] = normalized
            fixed_proxies += 1
    if fixed_proxies:
        log.info(f"[accounts] normalized {fixed_proxies} proxy URL(s) by prepending http://")
    return data


async def wallet_producer(state: State, task_queue: asyncio.Queue,
                          stats: dict, file_chunk: int = 1000):
    """Stream wallets.txt → BOUNDED task_queue. Skip already-checked via
    batched SQLite IN-query (no big set in RAM). Backpressure: blocks when
    queue is full so we never hold more than queue.maxsize items in memory.

    Memory: ~queue.maxsize * 200B  (~2 MB for maxsize=10000) instead of
    ~9 GB for the previous approach which preloaded all 47M wallets and
    a checked-wallets set into memory.

    Cancellation: clean — handles asyncio.CancelledError, marks done."""
    if not INPUT_WALLETS.exists():
        log.error(f"{INPUT_WALLETS} not found")
        stats["done"] = True
        sys.exit(1)

    file_size_mb = INPUT_WALLETS.stat().st_size / (1024 * 1024)
    progress_every = max(1000, int(getattr(state, "cfg_producer_progress_every_lines", 20000)))
    strict_wallet_validation = bool(getattr(state, "cfg_strict_wallet_validation", True))
    default_balance = str(getattr(state, "cfg_default_wallet_balance", "100"))
    next_progress = progress_every
    log.info(
        f"[producer] streaming from {INPUT_WALLETS} "
        f"(size={file_size_mb:.1f} MB, chunk={file_chunk}, "
        f"strict_wallet_validation={strict_wallet_validation}, "
        f"default_balance={default_balance!r})"
    )

    def _parse_chunk(lines):
        """Parse raw lines → list of (wallet, balance) tuples + count of invalid."""
        items = []
        invalid = 0
        for raw in lines:
            ln = raw.strip()
            if not ln or ln.startswith("#"):
                continue
            if ":" in ln:
                wallet, balance = ln.rsplit(":", 1)
                wallet = wallet.strip()
                balance = balance.strip() or default_balance
            else:
                wallet = ln
                balance = default_balance
            if not wallet:
                invalid += 1
                continue
            if strict_wallet_validation and not is_valid_wallet_candidate(wallet):
                invalid += 1
                continue
            items.append((wallet, balance))
        return items, invalid

    def _filter_already(items):
        """Run batched SQLite IN-query in a thread (sqlite is sync)."""
        addrs = [w.lower() for w, _ in items]
        already = state.filter_checked_subset(addrs)
        return [(w, b) for w, b in items if w.lower() not in already]

    try:
        with open(INPUT_WALLETS, "r", encoding="utf-8", errors="ignore") as f:
            chunk_lines = []
            for raw in f:
                chunk_lines.append(raw)
                stats["total_seen"] += 1

                if len(chunk_lines) >= file_chunk:
                    items, invalid = _parse_chunk(chunk_lines)
                    chunk_lines = []
                    stats["skipped_invalid"] += invalid
                    if items:
                        fresh = await asyncio.to_thread(_filter_already, items)
                        stats["skipped"] += len(items) - len(fresh)
                        for it in fresh:
                            await task_queue.put(it)  # blocks if full → backpressure
                            stats["queued"] += 1

                    if stats["total_seen"] >= next_progress:
                        log.info(
                            f"  [producer] read={stats['total_seen']} "
                            f"queued={stats['queued']} skipped={stats['skipped']} "
                            f"invalid={stats['skipped_invalid']} "
                            f"qsize={task_queue.qsize()}/{task_queue.maxsize}"
                        )
                        next_progress += progress_every

            # Tail chunk
            if chunk_lines:
                items, invalid = _parse_chunk(chunk_lines)
                stats["skipped_invalid"] += invalid
                if items:
                    fresh = await asyncio.to_thread(_filter_already, items)
                    stats["skipped"] += len(items) - len(fresh)
                    for it in fresh:
                        await task_queue.put(it)
                        stats["queued"] += 1

        log.info(
            f"[producer] DONE: read={stats['total_seen']} "
            f"queued={stats['queued']} skipped={stats['skipped']} "
            f"invalid={stats['skipped_invalid']}"
        )
    except asyncio.CancelledError:
        log.info(
            f"[producer] cancelled at read={stats['total_seen']} "
            f"queued={stats['queued']}"
        )
        raise
    finally:
        stats["done"] = True
        try:
            state.producer_done_hint = True
        except Exception:
            pass


async def wal_checkpointer(state: State, interval: int = 300):
    """Periodic WAL → main DB checkpoint. Keeps .wal file small, ensures
    that recent commits survive even hard kill (kill -9 / power loss)."""
    while True:
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return
        try:
            ok = await asyncio.to_thread(state.wal_checkpoint)
            if ok:
                log.info("[wal] checkpoint OK")
        except Exception as e:
            log.warning(f"[wal] checkpoint failed: {e}")


async def periodic_stats(task_queue: asyncio.Queue, active: dict,
                         pool: NitterPool, interval: int = 30, producer_stats: dict | None = None):
    while True:
        await asyncio.sleep(interval)
        pending = task_queue.qsize()
        api_cd = api_circuit_sleep_left()
        api_tag = "HOT" if api_cd <= 0 else f"COLD-{api_cd:.0f}s"
        prod = producer_stats or {}
        log.info(f"=== stats === pending={pending} alive={len(active)} "
                 f"api={api_tag} nitter={pool.healthy_count()}/{len(pool.instances)} "
                 f"producer_read={prod.get('total_seen','?')} queued={prod.get('queued','?')} "
                 f"skipped={prod.get('skipped','?')} done={prod.get('done','?')}")
        for lbl, w in list(active.items()):
            log.info(f"  {lbl}: remaining={w.rate_remaining}/50 health={w.health} "
                     f"engine={w.last_engine}")
        log.info(f"  [pool] {pool.summary()}")


async def amain():
    _load_parser_deps()
    log.info("=" * 70)
    log.info("Twitter wallet mention parser (dual-engine, passive cold-wait)")
    log.info(f"[build] {BUILD_TAG}")
    log.info("=" * 70)

    cfg = load_config()
    accounts = load_accounts()
    api_batch = int(cfg.get("api_batch_size", cfg.get("batch_size", 8)))
    nitter_batch = int(cfg.get("nitter_batch_size", cfg.get("batch_size", 8)))
    req_min = float(cfg.get("request_interval_min_s", 12))
    req_norm = float(cfg.get("request_interval_s", 16))
    api_expected_30m_fast = int(len(accounts) * api_batch * 1800 / max(1.0, req_min))
    api_expected_30m_norm = int(len(accounts) * api_batch * 1800 / max(1.0, req_norm))
    log.info(
        f"[speed] api_batch_size={api_batch} nitter_batch_size={nitter_batch} "
        f"api_expected_30m≈{api_expected_30m_norm}-{api_expected_30m_fast} "
        f"(API only, if accounts stay healthy)"
    )

    loop = asyncio.get_running_loop()
    loop.set_default_executor(
        ThreadPoolExecutor(
            max_workers=max(int(cfg.get("max_workers", 20)), len(accounts) + 4),
            thread_name_prefix="worker-io",
        )
    )

    state = State(STATE_DB)

    log.info("[setup] refreshing SearchTimeline query_id from x.com/main.js...")
    proxy_candidates = [a["proxy"] for a in accounts[:3]]
    new_qid = scrape_search_timeline_query_id(proxy_candidates)
    global QUERY_ID, SEARCH_URL
    if new_qid:
        if new_qid != QUERY_ID:
            log.info(f"[setup] query_id REFRESHED: {QUERY_ID} -> {new_qid}")
            QUERY_ID = new_qid
            SEARCH_URL = f"https://x.com/i/api/graphql/{QUERY_ID}/SearchTimeline"
        else:
            log.info(f"[setup] query_id unchanged: {QUERY_ID}")
    else:
        log.warning(f"[setup] using stale default query_id {QUERY_ID} "
                    f"(scrape failed — API calls may 404)")

    log.info("[dedup] loading bazaTwitters.txt into SQLite (batched, ~5k/txn)...")
    dedup_loaded = state.load_dedup_profiles_from_file(INPUT_DEDUP)
    log.info(f"[dedup] DONE — {dedup_loaded} profiles loaded")

    nitter_urls = cfg.get("nitter_instances") or []
    if not nitter_urls and cfg.get("nitter_url"):
        nitter_urls = [cfg["nitter_url"]]
    pool = NitterPool(nitter_urls, cfg)

    # BOUNDED queue — max ~2 MB RAM instead of ~9 GB.
    # Producer (wallet_producer) blocks on .put() when full → backpressure.
    QUEUE_MAX = int(cfg.get("task_queue_maxsize", 10000))
    task_queue = asyncio.Queue(maxsize=QUEUE_MAX)
    log.info(f"[input] task_queue bounded at maxsize={QUEUE_MAX}")

    out_queue = asyncio.Queue()
    db_queue = asyncio.Queue()
    active_workers = {}
    init_parallel = max(1, int(cfg.get("init_max_parallel", 6)))
    if cfg.get("startup_init_all_accounts", True):
        init_parallel = max(
            init_parallel,
            min(len(accounts), int(cfg.get("startup_init_min_parallel", 18))),
        )
    startup_attempts = max(1, int(cfg.get(
        "startup_init_attempts",
        cfg.get("init_attempts", 1),
    )))
    effective_init_timeout = min(
        float(cfg.get("init_timeout_s", 35)),
        float(cfg.get("init_timeout_cap_s", 35)),
    )
    effective_request_timeout = min(
        float(cfg.get("init_request_timeout_s", 12)),
        float(cfg.get("init_request_timeout_cap_s", 12)),
    )
    log.info(
        f"[init] parallel={init_parallel} startup_attempts={startup_attempts} "
        f"timeout={effective_init_timeout:g}s "
        f"request_timeout={effective_request_timeout:g}s"
    )
    init_sem = asyncio.Semaphore(init_parallel)

    tg = TelegramNotifier(cfg.get("telegram_bot_token", ""), cfg.get("telegram_chat_id", ""), cfg)
    if tg.enabled:
        log.info(f"[tg] enabled, chat_id={tg.chat_id}")
    else:
        log.info("[tg] disabled (bot_token or chat_id empty in config.json)")
    tg.note("started")
    n_inst = len(pool.instances) if pool else 0
    if tg.enabled:
        await tg.send(
            "<b>🚀 Парсер запущен</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📁 <b>Input:</b> <code>{_h(str(INPUT_WALLETS.name))}</code>\n"
            f"💾 <b>Results:</b> <code>{_h(str(OUTPUT_RESULTS.name))}</code>\n\n"
            f"👥 Аккаунтов: <b>{len(accounts)}</b>\n"
            f"🌐 Nitter: <b>{n_inst}</b> инстансов\n\n"
            "📦 Батчи: каждые <b>20 000</b> строк (закрепляются ✅)\n"
            "📊 Отчёт: по кнопке 📊 в боте (автоудаление)",
            pin=bool(cfg.get("telegram_pin_start", False)),
        )

    # Streaming producer — replaces old eager load. Reads file chunked,
    # filters checked via batched IN-query, blocks on full queue.
    producer_stats = {
        "total_seen": 0, "queued": 0, "skipped": 0,
        "skipped_invalid": 0, "done": False,
    }
    try:
        state.cfg_producer_progress_every_lines = int(cfg.get("producer_progress_every_lines", 20000))
    except Exception:
        state.cfg_producer_progress_every_lines = 20000
    state.cfg_strict_wallet_validation = bool(cfg.get("strict_wallet_validation", True))
    state.cfg_default_wallet_balance = str(cfg.get("default_wallet_balance", "100"))
    state.producer_done_hint = False

    producer_task = asyncio.create_task(
        wallet_producer(
            state,
            task_queue,
            producer_stats,
            int(cfg.get("producer_chunk_lines", 1000)),
        ),
        name="wallet-producer",
    )

    db_task = asyncio.create_task(
        db_writer(db_queue, state, int(cfg.get("db_batch_size", 100)))
    )
    writer = asyncio.create_task(output_writer(out_queue, db_queue))
    workers = [
        asyncio.create_task(worker_loop(a, state, cfg, task_queue, out_queue, db_queue,
                                         active_workers, tg, len(accounts), pool,
                                         init_sem))
        for a in accounts
    ]
    stats_task = asyncio.create_task(
        periodic_stats(task_queue, active_workers, pool,
                       cfg.get("dump_state_every_s", 30), producer_stats)
    )
    # Telegram auto-reports disabled — user checks status manually via bot buttons.
    # telegram_reporter() kept in code for reference but not started.
    try:
        state.cfg_target_per_5min = int(cfg.get("telegram_target_per_5min", 2200))
    except Exception:
        state.cfg_target_per_5min = 2200
    tg_task = None  # no auto-reporter
    tg_batch_task = asyncio.create_task(
        telegram_batch_exporter(tg, cfg),
        name="telegram-batch-exporter",
    )
    wal_task = asyncio.create_task(
        wal_checkpointer(state, int(cfg.get("wal_checkpoint_interval_s", 300))),
        name="wal-checkpointer",
    )

    # Publish state for TgBotController to read.
    _RUNTIME.update({
        "state": state, "active_workers": active_workers,
        "task_queue": task_queue, "pool": pool,
        "tg": tg, "producer_stats": producer_stats,
    })

    async def watch_workers():
        started_at = time.monotonic()
        grace_s = max(60.0, float(cfg.get("all_workers_dead_grace_s", 300)))
        while True:
            await asyncio.sleep(60)
            if active_workers:
                continue
            pending_workers = sum(1 for t in workers if not t.done())
            if pending_workers and time.monotonic() - started_at < grace_s:
                log.warning(
                    f"[main] no active workers yet, but {pending_workers} worker "
                    f"task(s) still starting; grace={grace_s:.0f}s"
                )
                continue
            if not active_workers:
                log.error(
                    f"[main] all workers dead/inactive, stopping "
                    f"(pending_worker_tasks={pending_workers})"
                )
                tg.note("all_dead")
                return

    async def wait_all_done():
        """Wait for producer to finish reading file AND queue to drain."""
        try:
            await producer_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"[producer] crashed: {e}")
        await task_queue.join()

    watcher = asyncio.create_task(watch_workers())
    all_done = asyncio.create_task(wait_all_done())
    try:
        await asyncio.wait(
            [all_done, watcher], return_when=asyncio.FIRST_COMPLETED
        )
    except KeyboardInterrupt:
        log.info("[main] interrupted, stopping workers...")
    finally:
        watcher.cancel()
        all_done.cancel()

    # Stop producer first so no new items enter the queue
    if not producer_task.done():
        producer_task.cancel()
        try:
            await producer_task
        except (asyncio.CancelledError, Exception):
            pass

    # Each worker has TWO sub-consumers (nitter + api), so 2 sentinels per worker.
    # Queue is bounded — use awaitable put().
    for _ in range(2 * len(workers)):
        await task_queue.put(None)
    await asyncio.gather(*workers, return_exceptions=True)

    await out_queue.put(None)
    await writer

    await db_queue.put(None)
    await db_task

    stats_task.cancel()
    if tg_task and not tg_task.done():
        tg_task.cancel()
    tg_batch_task.cancel()
    wal_task.cancel()
    gather_tasks = [stats_task, tg_batch_task, wal_task]
    if tg_task:
        gather_tasks.append(tg_task)
    try:
        await asyncio.gather(*gather_tasks, return_exceptions=True)
    except Exception:
        pass

    # Final WAL checkpoint — guarantees everything is in main .db file,
    # so even a hard kill right after this point loses nothing.
    try:
        await asyncio.to_thread(state.wal_checkpoint)
        log.info("[wal] final checkpoint OK")
    except Exception as e:
        log.warning(f"[wal] final checkpoint failed: {e}")

    total_checked = state.count_checked()
    total_results = state.get_counter("results_written")
    results_in_file = count_output_results()
    tg.note("finished")
    if tg.enabled:
        await tg.send(
            "<b>🏁 Парсер завершился</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💾 Проверено: <b>{total_checked}</b>\n"
            f"🎯 Результатов: <b>{results_in_file}</b>\n"
            f"🧾 Записано: <b>{total_results}</b>\n\n"
            "✅ State сохранён — следующий запуск продолжит"
        )

    _RUNTIME.clear()
    log.info("Done.")


def main():
    OUTPUT_RESULTS.parent.mkdir(parents=True, exist_ok=True)
    INPUT_DEDUP.parent.mkdir(parents=True, exist_ok=True)
    _setup_logging(OUTPUT_ERRORS)
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        log.info("Interrupted. State saved in state.db — next run resumes automatically.")


# ---------------------------------------------------------------------------
#  Telegram Bot Controller (inline-кнопки, управление парсером)
# ---------------------------------------------------------------------------

def _fmt_num(n) -> str:
    return f"{int(n):,}".replace(",", " ")


def _fmt_uptime(seconds: float) -> str:
    s = int(seconds)
    if s < 60:
        return f"{s}с"
    h, remainder = divmod(s, 3600)
    m = remainder // 60
    if h:
        return f"{h}ч {m}м"
    return f"{m}м {s % 60}с"


class TgBotController:
    """Full Telegram bot with inline-button UI for managing the parser."""

    STAT_AUTO_DELETE_S = 60  # auto-delete stats after N seconds

    def __init__(self, tg: TelegramNotifier, cfg: dict):
        self.tg = tg
        self.cfg = cfg
        self.offset = 0
        self.parser_task: asyncio.Task | None = None
        self.parser_running = False
        self.started_at: float = 0.0
        self._crash_reason = ""
        self._last_checked = 0
        self._last_report_ts = 0.0
        allowed_raw = cfg.get("telegram_allowed_user_ids", [])
        self.allowed_ids = set(int(x) for x in allowed_raw) if allowed_raw else set()

    # ── Keyboard builders ─────────────────────────────────

    def _kb_main(self) -> dict:
        if self.parser_running:
            return {"inline_keyboard": [
                [{"text": "⏹ Стоп", "callback_data": "stop"},
                 {"text": "📊 Статус", "callback_data": "status"}],
                [{"text": "🚀 Скорость", "callback_data": "speed"},
                 {"text": "👥 Аккаунты", "callback_data": "accounts"}],
                [{"text": "🌐 Nitter", "callback_data": "nitter"},
                 {"text": "⚙️ Конфиг", "callback_data": "config"}],
            ]}
        return {"inline_keyboard": [
            [{"text": "▶️ Запустить парсер", "callback_data": "run"}],
            [{"text": "⚙️ Конфиг", "callback_data": "config"},
             {"text": "❓ Помощь", "callback_data": "help"}],
        ]}

    # ── Status helpers ────────────────────────────────────

    def _get_status(self) -> dict:
        info = {
            "checked": 0, "results": 0, "queue_size": 0,
            "alive_workers": 0, "nitter_healthy": 0, "nitter_total": 0,
            "api_status": "—", "total_results": 0,
        }
        try:
            rt = _RUNTIME
            state = rt.get("state")
            active = rt.get("active_workers", {})
            queue = rt.get("task_queue")
            pool = rt.get("pool")
            if state:
                info["checked"] = state.count_checked()
                info["total_results"] = state.get_counter("results_written")
            info["results"] = count_output_results()
            if queue:
                info["queue_size"] = queue.qsize()
            info["alive_workers"] = len(active)
            if pool:
                info["nitter_healthy"] = pool.healthy_count()
                info["nitter_total"] = len(pool.instances)
            api_cd = api_circuit_sleep_left()
            info["api_status"] = "🟢 HOT" if api_cd <= 0 else f"🔴 COLD {api_cd:.0f}s"
        except Exception as e:
            log.warning(f"[bot] status read error: {e}")
        return info

    # ── Parser engine ─────────────────────────────────────

    async def _run_parser(self):
        try:
            await amain()
        except asyncio.CancelledError:
            log.info("[bot] parser cancelled")
            await self.tg.send(
                "<b>⏹ Парсер остановлен</b>",
                reply_markup=self._kb_main())
        except Exception as e:
            self._crash_reason = str(e)[:300]
            log.error(f"[bot] parser crashed: {e}\n{traceback.format_exc()}")
            err_text = _h(str(e)[:200])
            await self.tg.send(
                f"<b>❌ Парсер упал!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<code>{err_text}</code>\n\n"
                f"Нажми ▶️ чтобы перезапустить",
                reply_markup=self._kb_main())
        finally:
            self.parser_running = False

    async def start_parser(self) -> tuple[bool, str]:
        if self.parser_running:
            return False, "Парсер уже работает"
        self.parser_running = True
        self.started_at = time.time()
        self._crash_reason = ""
        OUTPUT_RESULTS.parent.mkdir(parents=True, exist_ok=True)
        INPUT_DEDUP.parent.mkdir(parents=True, exist_ok=True)
        self.parser_task = asyncio.create_task(self._run_parser(), name="parser")
        return True, "Парсер запускается..."

    async def stop_parser(self) -> tuple[bool, str]:
        if not self.parser_running:
            return False, "Парсер не запущен"
        self.parser_running = False
        if self.parser_task and not self.parser_task.done():
            self.parser_task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(self.parser_task), timeout=30)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass
        self.parser_task = None
        return True, "Парсер остановлен"

    def _uptime(self) -> str:
        if not self.parser_running or not self.started_at:
            return "—"
        return _fmt_uptime(time.time() - self.started_at)

    # ── Message builders ──────────────────────────────────

    def _build_welcome(self, name: str = "", uid: int = 0) -> str:
        return (
            f"<b>🤖 Wallet Parser Bot</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Привет{', <b>' + _h(name) + '</b>' if name else ''}!\n\n"
            f"Управление парсером кнопками ниже.\n\n"
            f"{'🟢 <b>Работает</b> | ⏱ ' + self._uptime() if self.parser_running else '🔴 <b>Остановлен</b>'}\n\n"
            f"<i>ID: <code>{uid}</code></i>"
        )

    def _build_status(self) -> str:
        if not self.parser_running:
            text = (
                "<b>📊 Статус парсера</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "🔴 <b>Парсер не запущен</b>\n\n"
                "Нажми ▶️ чтобы запустить"
            )
            if self._crash_reason:
                text += f"\n\n⚠️ <b>Последняя ошибка:</b>\n<code>{_h(self._crash_reason[:200])}</code>"
            return text
        s = self._get_status()
        now = time.time()
        elapsed = now - self._last_report_ts if self._last_report_ts else 0
        delta = s["checked"] - self._last_checked
        period_min = max(1, int(elapsed / 60)) if elapsed > 0 else 0
        per_5_min = int(delta * 300 / max(1, elapsed)) if elapsed > 0 else 0
        target_5m = int(self.cfg.get("telegram_target_per_5min", 2200))
        target_tag = "✅" if per_5_min >= target_5m else "⚠️"
        pct = min(100, int(per_5_min / max(1, target_5m) * 100))
        bar = "▓" * (pct // 10) + "░" * (10 - pct // 10)

        rt_tg = _RUNTIME.get("tg", self.tg)
        events = rt_tg.drain_events()
        event_lines = []
        if events.get("api_paused"):
            event_lines.append(f"⏸ API пауз: {events['api_paused']}")
        if events.get("api_recovered"):
            event_lines.append(f"✅ API восстановилось: {events['api_recovered']}")
        if events.get("init_failed"):
            event_lines.append(f"⚠️ Init fail: {events['init_failed']}")
        if events.get("nitter_only"):
            event_lines.append(f"🟡 Nitter-only: {events['nitter_only']}")
        if events.get("retired"):
            event_lines.append(f"🔴 Акков снято: {events['retired']}")
        if events.get("all_dead"):
            event_lines.append("🚨 Все воркеры мертвы")
        events_text = "\n".join(event_lines) if event_lines else "🟢 Всё спокойно"

        # Update tracking for next press
        self._last_checked = s["checked"]
        self._last_report_ts = now

        period_label = f" за {period_min} мин" if period_min else ""
        return (
            f"<b>📊 Отчёт парсера</b>{period_label}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🟢 <b>Работает</b> | ⏱ {self._uptime()}\n\n"
            f"<b>⚡ Скорость</b>\n"
            f"├ За период: <b>+{_fmt_num(delta)}</b>\n"
            f"├ Темп: <b>~{per_5_min}</b>/5мин ({target_tag} цель {target_5m})\n"
            f"└ <code>[{bar}]</code> {pct}%\n\n"
            f"<b>📈 Прогресс</b>\n"
            f"├ 💾 В базе: <b>{_fmt_num(s['checked'])}</b>\n"
            f"├ 🎯 Результатов: <b>{_fmt_num(s['results'])}</b>\n"
            f"└ 📦 Очередь: <b>{_fmt_num(s['queue_size'])}</b>\n\n"
            f"<b>⚙️ Инфра</b>\n"
            f"├ 🧠 API: {s['api_status']}\n"
            f"├ 👥 Воркеры: <b>{s['alive_workers']}</b>\n"
            f"└ 🌐 Nitter: <b>{s['nitter_healthy']}/{s['nitter_total']}</b>\n\n"
            f"<b>📌 События:</b>\n{events_text}\n\n"
            f"<i>🗑 Удалится через {self.STAT_AUTO_DELETE_S}с или нажми ✖️</i>"
        )

    def _build_speed(self) -> str:
        if not self.parser_running:
            return "🔴 <b>Парсер не запущен</b>"
        s = self._get_status()
        uptime_s = time.time() - self.started_at if self.started_at else 1
        rate_min = s["checked"] / max(1, uptime_s / 60)
        rate_5 = rate_min * 5
        pct = min(100, int(rate_5 / max(1, 2200) * 100))
        bar = "▓" * (pct // 10) + "░" * (10 - pct // 10)
        eta = "—"
        if rate_min > 0 and s["queue_size"] > 0:
            eta = _fmt_uptime(s["queue_size"] / rate_min * 60)
        return (
            f"<b>🚀 Скорость парсера</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⏱ Аптайм: <b>{self._uptime()}</b>\n\n"
            f"<b>📊 Темп</b>\n"
            f"├ В минуту: <b>~{int(rate_min)}</b>\n"
            f"├ За 5 мин: <b>~{int(rate_5)}</b> (цель 2 200)\n"
            f"└ <code>[{bar}]</code> {pct}%\n\n"
            f"<b>⏳ Оценка</b>\n"
            f"├ Осталось: <b>{_fmt_num(s['queue_size'])}</b>\n"
            f"└ ETA: <b>{eta}</b>"
        )

    def _build_accounts(self) -> str:
        if not self.parser_running:
            return "🔴 <b>Парсер не запущен</b>"
        workers = list(_RUNTIME.get("active_workers", {}).items())
        if not workers:
            return (
                "<b>👥 Аккаунты</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "⏳ Аккаунты инициализируются..."
            )
        lines = []
        for label, w in workers:
            health = getattr(w, "health", 0)
            hearts = "❤️" * max(0, health) + "🖤" * max(0, 3 - health)
            rate = getattr(w, "rate_remaining", 0)
            rate_icon = "🟢" if rate > 30 else ("🟡" if rate > 10 else "🔴")
            eng = getattr(w, "last_engine", "?")
            lines.append(
                f"├ <b>{_h(label)}</b>\n"
                f"│  {hearts} | {rate_icon} {rate}/50 | {_h(eng)}"
            )
        return (
            f"<b>👥 Аккаунты ({len(workers)})</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            + "\n".join(lines)
        )

    def _build_nitter(self) -> str:
        pool = _RUNTIME.get("pool")
        if not self.parser_running or not pool:
            return "🔴 <b>Парсер не запущен</b>"
        if not pool.instances:
            return "<b>🌐 Nitter</b>\n━━━━━━━━━━━━━━━━━━━━\n\nНе сконфигурирован"
        now = time.monotonic()
        lines = []
        for inst in pool.instances:
            cd = max(0.0, inst.cold_until - now)
            status = "🟢 HOT" if cd == 0 else f"🔴 COLD {cd:.0f}s"
            ok = inst.total_success
            fail = inst.total_fail
            total = ok + fail
            pct = int(ok / max(1, total) * 100) if total else 0
            lines.append(
                f"├ <b>{_h(inst.host)}</b>\n"
                f"│  {status} | ✅ {ok} ❌ {fail} ({pct}%)"
            )
        return (
            f"<b>🌐 Nitter ({pool.healthy_count()}/{len(pool.instances)})</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            + "\n".join(lines)
        )

    def _build_config(self) -> str:
        safe_keys = [
            "since_date", "api_batch_size", "nitter_batch_size",
            "request_interval_s", "request_interval_min_s",
            "request_interval_max_s", "nitter_enabled",
            "telegram_report_interval_s", "telegram_batch_export_size",
            "telegram_pin_batches", "telegram_pin_reports",
            "strict_wallet_validation",
        ]
        lines = []
        for k in safe_keys:
            if k in self.cfg:
                v = self.cfg[k]
                if isinstance(v, list):
                    v = f"[{len(v)} шт]"
                lines.append(f"├ <code>{k}</code> = <b>{_h(str(v))}</b>")
        if not lines:
            lines.append("├ <i>config.json пуст</i>")
        return (
            "<b>⚙️ Конфигурация</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            + "\n".join(lines) + "\n\n"
            "<i>Изменить: /set key value</i>"
        )

    # ── Auto-delete stat messages ─────────────────────────

    async def _send_temp_stat(self, text: str):
        """Send a stat message with ✖️ dismiss button and auto-delete timer."""
        kb = {"inline_keyboard": [
            [{"text": "✖️ Закрыть", "callback_data": "dismiss:0"}],
        ]}
        mid = await self.tg.send(text, reply_markup=kb)
        if mid and isinstance(mid, int):
            # Update the dismiss button with actual message_id
            kb["inline_keyboard"][0][0]["callback_data"] = f"dismiss:{mid}"
            await self.tg.edit_message(mid, text, reply_markup=kb)
            # Schedule auto-delete
            async def _auto_del():
                await asyncio.sleep(self.STAT_AUTO_DELETE_S)
                await self.tg.delete_message(mid)
            asyncio.create_task(_auto_del())

    # ── Dispatch ──────────────────────────────────────────

    def _check_access(self, uid: int) -> bool:
        if not self.allowed_ids:
            return True
        return uid in self.allowed_ids

    async def _handle_update(self, upd: dict):
        try:
            if "callback_query" in upd:
                await self._handle_callback(upd["callback_query"])
            elif "message" in upd:
                await self._handle_message(upd["message"])
        except Exception as e:
            log.error(f"[bot] handle_update error: {e}\n{traceback.format_exc()}")
            try:
                await self.tg.send(
                    f"<b>⚠️ Ошибка бота:</b>\n<code>{_h(str(e)[:200])}</code>",
                    reply_markup=self._kb_main())
            except Exception:
                pass

    async def _handle_message(self, msg: dict):
        uid = (msg.get("from") or {}).get("id", 0)
        text = msg.get("text", "")

        if not self._check_access(uid):
            await self.tg.send(
                f"⛔ <b>Нет доступа</b>\nID: <code>{uid}</code>",
                reply_markup=None)
            return

        if text.startswith("/start") or text.startswith("/help"):
            name = (msg.get("from") or {}).get("first_name", "")
            await self.tg.send(
                self._build_welcome(name, uid),
                reply_markup=self._kb_main())

        elif text.startswith("/set "):
            parts = text.split(maxsplit=2)
            if len(parts) < 3:
                await self.tg.send(
                    "⚠️ <b>Формат:</b> <code>/set key value</code>",
                    reply_markup=self._kb_main())
                return
            key, raw_val = parts[1], parts[2]
            if raw_val.lower() in ("true", "yes", "1"):
                value = True
            elif raw_val.lower() in ("false", "no", "0"):
                value = False
            else:
                try:
                    value = int(raw_val)
                except ValueError:
                    try:
                        value = float(raw_val)
                    except ValueError:
                        value = raw_val
            old = self.cfg.get(key, "<не задано>")
            self.cfg[key] = value
            try:
                CONFIG_FILE.write_text(
                    json.dumps(self.cfg, indent=2, ensure_ascii=False),
                    encoding="utf-8")
            except Exception as e:
                log.warning(f"[bot] config save error: {e}")
            await self.tg.send(
                f"✅ <b>Обновлено</b>\n\n"
                f"<code>{_h(key)}</code>\n"
                f"├ Было: <code>{_h(str(old))}</code>\n"
                f"└ Стало: <b>{_h(str(value))}</b>\n\n"
                f"<i>⚠️ Некоторые настройки требуют рестарта</i>",
                reply_markup=self._kb_main())

        elif text.startswith("/"):
            await self.tg.send(
                "❓ Используй кнопки для управления\nили /set для настроек",
                reply_markup=self._kb_main())

    async def _handle_callback(self, cb: dict):
        uid = (cb.get("from") or {}).get("id", 0)
        mid = cb["message"]["message_id"]
        data = cb.get("data", "")
        log.info(f"[bot] callback: data={data!r} uid={uid}")

        if not self._check_access(uid):
            await self.tg.answer_callback(cb["id"], "⛔ Нет доступа")
            return

        await self.tg.answer_callback(cb["id"])

        if data == "run":
            log.info("[bot] starting parser via button...")
            ok, msg = await self.start_parser()
            log.info(f"[bot] start_parser result: ok={ok} msg={msg}")
            if ok:
                await self.tg.send(
                    "<b>▶️ Парсер запускается...</b>\n\n"
                    "⏳ Инициализация аккаунтов X...\n"
                    "Это может занять 30–60 секунд.",
                    reply_markup=self._kb_main())
            else:
                await self.tg.send(
                    f"⚠️ {_h(msg)}",
                    reply_markup=self._kb_main())

        elif data == "stop":
            await self.tg.edit_message(mid, "⏳ <b>Останавливаю...</b>")
            ok, msg = await self.stop_parser()
            await self.tg.edit_message(mid,
                f"{'⏹' if ok else '⚠️'} <b>{_h(msg)}</b>",
                reply_markup=self._kb_main())

        elif data == "status":
            await self._send_temp_stat(self._build_status())

        elif data == "speed":
            await self._send_temp_stat(self._build_speed())

        elif data == "accounts":
            await self._send_temp_stat(self._build_accounts())

        elif data == "nitter":
            await self._send_temp_stat(self._build_nitter())

        elif data == "config":
            await self.tg.send(self._build_config(),
                               reply_markup=self._kb_main())

        elif data == "help":
            name = (cb.get("from") or {}).get("first_name", "")
            await self.tg.send(self._build_welcome(name, uid),
                               reply_markup=self._kb_main())

        elif data == "menu":
            status_emoji = "🟢 Работает" if self.parser_running else "🔴 Остановлен"
            await self.tg.edit_message(mid,
                f"<b>🤖 Wallet Parser Bot</b>\n\n{status_emoji}",
                reply_markup=self._kb_main())

        elif data.startswith("dismiss:"):
            try:
                del_mid = int(data.split(":", 1)[1])
                await self.tg.delete_message(del_mid)
            except Exception:
                pass

    # ── Main loop ─────────────────────────────────────────

    async def run(self):
        log.info("=" * 50)
        log.info("Wallet Parser — Telegram Bot Mode")
        log.info("=" * 50)

        await self.tg.set_my_commands([
            {"command": "start", "description": "🏠 Главное меню"},
            {"command": "set", "description": "⚙️ Изменить настройку"},
            {"command": "help", "description": "❓ Помощь"},
        ])

        await self.tg.send(
            "<b>🤖 Бот запущен</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Управляй парсером кнопками ниже.",
            reply_markup=self._kb_main())

        log.info("[bot] long-polling started, waiting for button presses...")
        try:
            while True:
                try:
                    updates = await self.tg.get_updates(self.offset, timeout=30)
                    for upd in updates:
                        self.offset = upd["update_id"] + 1
                        asyncio.create_task(self._handle_update(upd))
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning(f"[bot] poll error: {e}")
                    await asyncio.sleep(5)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            if self.parser_running:
                await self.stop_parser()


async def abot():
    """Entry point for bot mode."""
    cfg = load_config()
    tg = TelegramNotifier(
        cfg.get("telegram_bot_token", ""),
        cfg.get("telegram_chat_id", ""),
        cfg,
    )
    if not tg.enabled:
        print("=" * 50)
        print("ОШИБКА: telegram_bot_token или telegram_chat_id не заданы!")
        print("Укажите в config.json")
        print("=" * 50)
        sys.exit(1)
    bot = TgBotController(tg, cfg)
    await bot.run()


if __name__ == "__main__":
    if "--bot" in sys.argv:
        _setup_logging(OUTPUT_ERRORS)
        try:
            asyncio.run(abot())
        except KeyboardInterrupt:
            log.info("Bot stopped.")
    else:
        main()