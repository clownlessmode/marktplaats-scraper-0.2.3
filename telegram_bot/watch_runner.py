"""Режим слежения: парсер работает постоянно, ищет объявления младше 3 часов, сразу отправляет в Telegram."""
import logging
import os
import queue
import shutil
import sys
import threading
from dataclasses import asdict

from mpscraper.mpscraper import MpScraper
from mpscraper.listing import Listing
from mpscraper.exceptions import CategoryStale, ProxyError

from .database import init_db, load_listings_from_db, upsert_listings

logger = logging.getLogger(__name__)

WATCH_MAX_AGE_HOURS = 3.0
WATCH_INTERVAL_SECONDS = 60
WATCH_LIMIT_PER_CATEGORY = 30  # сколько проверять в каждой категории


def _default_chromium_path() -> str:
    path = shutil.which("chromium")
    if path:
        return path
    if sys.platform == "darwin":
        for c in ("/opt/homebrew/bin/chromium", "/usr/local/bin/chromium"):
            if os.path.isfile(c):
                return c
    return "/usr/bin/chromium"


def run_watch_loop(
    db_path: str,
    chat_id: int,
    new_listing_queue: queue.Queue,
    stop_event: threading.Event,
    chromium_path: str | None = None,
    chromedriver_path: str | None = None,
    headless: bool = True,
    proxy: str | None = None,
) -> None:
    """Запускает бесконечный цикл слежения. При нахождении нового объявления кладёт его в new_listing_queue."""
    init_db(db_path)
    _, sent_item_ids = load_listings_from_db(db_path)  # не отправлять то, что уже в БД
    mp_scraper = None
    chromium_path = chromium_path or _default_chromium_path()

    def on_new(listing: Listing) -> None:
        if listing.item_id in sent_item_ids:
            return
        sent_item_ids.add(listing.item_id)
        import pandas as pd
        df = pd.DataFrame([asdict(listing)])
        try:
            upsert_listings(df, db_path)
        except Exception as e:
            logger.exception("Ошибка сохранения: %s", e)
        d = asdict(listing)
        for k, v in list(d.items()):
            if isinstance(v, tuple):
                d[k] = "|".join(str(x) for x in v)
        new_listing_queue.put((chat_id, d))

    while not stop_event.is_set():
        try:
            if mp_scraper is None:
                mp_scraper = MpScraper(
                    chromium_path=chromium_path,
                    chromedriver_path=chromedriver_path or "",
                    headless=headless,
                    timeout_seconds=10,
                    wait_seconds=2,
                    skip_cookies=True,
                    skip_count=True,
                    fast=False,  # нужны реальные timestamp для фильтра по возрасту
                    proxy=proxy,
                )

            parent_categories = mp_scraper.get_parent_categories()
            for parent in parent_categories:
                if stop_event.is_set():
                    break
                try:
                    mp_scraper.get_listings(
                        parent_category=parent,
                        limit=WATCH_LIMIT_PER_CATEGORY,
                        existing_item_ids=set(),
                        max_age_hours=WATCH_MAX_AGE_HOURS,
                        on_new_listing=on_new,
                    )
                except CategoryStale:
                    logger.debug("Категория %s: старые объявления, переходим дальше", parent.url.split("/")[-1])
                    continue

        except ProxyError:
            if mp_scraper:
                try:
                    mp_scraper.close()
                except Exception:
                    pass
                mp_scraper = None
            logger.warning("Прокси не работает, пауза 30 сек")
            stop_event.wait(30)
            continue
        except Exception as exc:
            logger.exception("Ошибка в watch: %s", exc)
            if mp_scraper:
                try:
                    mp_scraper.close()
                except Exception:
                    pass
                mp_scraper = None

        stop_event.wait(WATCH_INTERVAL_SECONDS)

    if mp_scraper:
        try:
            mp_scraper.close()
        except Exception:
            pass
