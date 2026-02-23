import json
import os
import shutil
import sys
import logging
import signal
import argparse
from pathlib import Path
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from typing import NamedTuple
import pandas as pd
from selenium.common.exceptions import TimeoutException, WebDriverException
from pyvirtualdisplay.display import Display
from tqdm import tqdm
from datetime import datetime

from .utils import (
    diff_hours,
    handle_sigterm_interrupt,
    remove_duplicate_listings,
    get_utc_now,
)
from .display import has_display, get_virtual_display
from .exceptions import (
    CategoriesError,
    CategoryStale,
    ForbiddenError,
    NotFoundError,
    ListingsError,
    ListingsInterrupt,
    ProxyError,
)
from .mpscraper import MpScraper, MARTKPLAATS_BASE_URL
from .listing import Listing
from .driver import MPDriver


def _listings_to_df(lst: list[Listing]) -> pd.DataFrame:
    """Конвертирует list[Listing] в DataFrame для БД."""
    if not lst:
        return pd.DataFrame()
    return pd.DataFrame([asdict(x) for x in lst])


ENV_PREFIX = "MP_"
ENV_LIMIT = f"{ENV_PREFIX}LIMIT"
ENV_HEADLESS = f"{ENV_PREFIX}HEADLESS"
ENV_TIMEOUT_SECONDS = f"{ENV_PREFIX}TIMEOUT_SECONDS"
ENV_RECRAWL_HOURS = f"{ENV_PREFIX}RECRAWL_HOURS"
ENV_USE_PROXIES = f"{ENV_PREFIX}USE_PROXIES"
ENV_WAIT_SECONDS = f"{ENV_PREFIX}WAIT_SECONDS"

DEFAULT_LIMIT = 0
BATCH_SAVE_SIZE = 50  # сохранять CSV каждые N новых объявлений
DEFAULT_TIMEOUT_SECONDS = 5

PROXIES_FILE = "proxies.json"  # JSON-массив строк "host:port", по одному на строку
DEFAULT_WAIT_SECONDS = 5
DEFAULT_DATA_DIR = "./"
DEFAULT_LISTINGS_FILE = "listings.csv"
DEFAULT_HEADLESS = False


def _get_default_chromium_path() -> str:
    """Detect Chromium path: PATH, then macOS Homebrew locations, else Linux default."""
    path = shutil.which("chromium")
    if path:
        return path
    if sys.platform == "darwin":
        for candidate in ("/opt/homebrew/bin/chromium", "/usr/local/bin/chromium"):
            if os.path.isfile(candidate):
                return candidate
    return "/usr/bin/chromium"


DEFAULT_CHROMIUM_PATH = _get_default_chromium_path()
DEFAULT_RECRAWL_HOURS = 24


def _load_proxies() -> list[str]:
    """Загрузить прокси из proxies.json (массив строк host:port)."""
    for base in (Path.cwd(), Path(__file__).resolve().parent.parent):
        path = base / PROXIES_FILE
        if path.is_file():
            try:
                raw = path.read_text(encoding="utf-8").strip()
                if not raw:
                    return []
                data = json.loads(raw)
                if isinstance(data, list):
                    return [str(p).strip() for p in data if p]
                return []
            except (json.JSONDecodeError, OSError) as e:
                logging.warning("Не удалось загрузить %s: %s", path, e)
    return []


class Args(NamedTuple):
    """Command-line arguments."""

    data_dir: str
    db_path: str
    limit: int
    headless: bool
    chromium_path: str
    chromedriver_path: str
    timeout_seconds: int
    wait_seconds: int
    recrawl_hours: float
    skip_cookies: bool
    skip_count: bool
    workers: int
    debug: bool
    track_clicks: bool
    block_css: bool
    proxies: tuple[str, ...]


class DisplayNotFound(Exception):
    """DisplayNotFound is raised when a display is not found in the given environment."""

    pass


def get_args() -> Args:
    """Return command-line arguments."""
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"The limit of new listings to scrape. ({ENV_LIMIT})",
    )

    parser.add_argument(
        "--headless",
        action="store_true",
        default=DEFAULT_HEADLESS,
        help=f"Run browser in headless mode. ({ENV_HEADLESS})",
    )

    parser.add_argument(
        "--chromium-path",
        type=str,
        default=DEFAULT_CHROMIUM_PATH,
        help="Path to Chromium executable.",
    )

    parser.add_argument(
        "--driver-path",
        type=str,
        help="Path to Chromium ChromeDriver executable.",
    )

    parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Seconds before timeout occurs. ({ENV_TIMEOUT_SECONDS})",
    )

    parser.add_argument(
        "--recrawl-hours",
        "-r",
        type=float,
        default=DEFAULT_RECRAWL_HOURS,
        help=f"Recrawl listings that haven't been checked for this many hours or more ({ENV_RECRAWL_HOURS})",
    )

    parser.add_argument(
        "--data-dir",
        "-d",
        type=str,
        default=DEFAULT_DATA_DIR,
        help="Directory to save output data.",
    )

    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=DEFAULT_WAIT_SECONDS,
        help=f"Seconds to wait before re-trying after being rate-limited. ({ENV_WAIT_SECONDS})",
    )

    parser.add_argument(
        "--skip-cookies",
        action="store_true",
        help="Skip auto cookie acceptance. Browser opens, you accept cookies manually, then press Enter in terminal.",
    )

    parser.add_argument(
        "--skip-count",
        action="store_true",
        help="Skip listings count fetch per category (saves 1 page load per category, faster for large limits).",
    )

    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=1,
        help="Number of parallel workers (browsers) for scraping categories.",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Save page HTML to debug_*.html when parsing fails (for structure inspection).",
    )

    parser.add_argument(
        "--track-clicks",
        action="store_true",
        help="Track clicks: open page, you click cookie button, we log element info for selector.",
    )

    parser.add_argument(
        "--proxy",
        "-p",
        nargs="*",
        default=None,
        help="Override PROXIES from code. Worker N uses proxy[N] if exists.",
    )
    parser.add_argument(
        "--socks5",
        action="store_true",
        help="Использовать SOCKS5 вместо HTTP (если прокси работают только по SOCKS5).",
    )

    parser.add_argument(
        "--block-css",
        action="store_true",
        help="Блокировать загрузку CSS (ускорение парсинга, экономия трафика).",
    )

    parser.add_argument(
        "--db-path",
        type=str,
        default="",
        help="Путь к SQLite БД для сохранения объявлений (по умолчанию {data-dir}/bot.db). Без CSV.",
    )

    args = parser.parse_args()
    if args.socks5:
        os.environ["MP_PROXY_TYPE"] = "socks5"
    env_limit = os.getenv(ENV_LIMIT)
    env_headless = os.getenv(ENV_HEADLESS)
    env_timeout_seconds = os.getenv(ENV_TIMEOUT_SECONDS)
    env_recrawl_hours = os.getenv(ENV_RECRAWL_HOURS)
    env_wait_seconds = os.getenv(ENV_WAIT_SECONDS)

    env_proxy = os.getenv("MP_PROXY")
    proxies: list[str] = list(args.proxy) if args.proxy is not None else _load_proxies()
    if env_proxy:
        proxies = [p.strip() for p in env_proxy.split(",") if p.strip()]

    data_dir: str = args.data_dir
    db_path: str = args.db_path or os.path.join(data_dir, "bot.db")
    limit: int = int(env_limit) if env_limit else args.limit
    headless: bool = bool(env_headless) if env_headless else args.headless
    chromium_path: str = args.chromium_path
    chromedriver_path: str = args.driver_path
    timeout_seconds: int = int(env_timeout_seconds) if env_timeout_seconds else args.timeout
    recrawl_hours: float = float(env_recrawl_hours) if env_recrawl_hours else args.recrawl_hours
    wait_seconds: int = int(env_wait_seconds) if env_wait_seconds else args.wait_seconds

    return Args(
        data_dir=data_dir,
        db_path=db_path,
        limit=limit,
        headless=headless,
        chromium_path=chromium_path,
        chromedriver_path=chromedriver_path,
        timeout_seconds=timeout_seconds,
        wait_seconds=wait_seconds,
        recrawl_hours=recrawl_hours,
        skip_cookies=args.skip_cookies,
        skip_count=args.skip_count,
        workers=max(1, args.workers),
        debug=args.debug,
        track_clicks=args.track_clicks,
        block_css=args.block_css,
        proxies=tuple(proxies),
    )


def _worker_scrape_categories(
    worker_id: int,
    categories: list,
    limit: int,
    item_ids: set[str],
    args: Args,
    progress_queue: queue.Queue | None = None,
) -> list[Listing]:
    """Воркер: обрабатывает свои категории, возвращает список объявлений."""
    all_listings: list[Listing] = []
    # Прокси для воркера: с worker_id по кругу, при сбое — следующий
    proxies_to_try = list(args.proxies) if args.proxies else [None]
    if proxies_to_try != [None]:
        proxies_to_try = proxies_to_try[worker_id:] + proxies_to_try[:worker_id]

    for proxy in proxies_to_try:
        mp_scraper = None
        try:
            if proxy:
                logging.info("Воркер %d: прокси...", worker_id)
            mp_scraper = MpScraper(
                chromium_path=args.chromium_path,
                chromedriver_path=args.chromedriver_path,
                headless=args.headless,
                timeout_seconds=args.timeout_seconds,
                wait_seconds=args.wait_seconds,
                skip_cookies=args.skip_cookies,
                skip_count=args.skip_count,
                debug=args.debug,
                fast=False,
                block_css=args.block_css,
                proxy=proxy,
            )
            worker_item_ids = item_ids.copy()
            for parent_category in categories:
                try:
                    listings = mp_scraper.get_listings(
                        parent_category=parent_category,
                        limit=limit,
                        existing_item_ids=worker_item_ids,
                        max_age_hours=3.0,
                    )
                    for listing in listings:
                        worker_item_ids.add(listing.item_id)
                    all_listings.extend(listings)
                    if progress_queue and listings:
                        progress_queue.put(listings)
                except CategoryStale as exc:
                    logging.info(
                        "Воркер %d: собрано %d, объявление >3ч → следующая категория",
                        worker_id,
                        len(exc.listings),
                    )
                    for listing in exc.listings:
                        worker_item_ids.add(listing.item_id)
                    all_listings.extend(exc.listings)
                    if progress_queue and exc.listings:
                        progress_queue.put(exc.listings)
                    continue
                except (ListingsError, ListingsInterrupt) as exc:
                    all_listings.extend(exc.listings)
                    if progress_queue and exc.listings:
                        progress_queue.put(exc.listings)
                    break
                except ProxyError as exc:
                    logging.info("Воркер %d: прокси не работает → смена", worker_id)
                    raise
                except ForbiddenError as fe:
                    if proxy:
                        logging.info("Воркер %d: 403 → смена прокси", worker_id)
                        raise ProxyError(proxy, fe.msg or "403") from fe
                    raise
            break  # успех
        except ProxyError:
            if mp_scraper:
                try:
                    mp_scraper.close()
                except Exception:
                    pass
            continue
        except (WebDriverException, TimeoutException) as exc:
            if proxy and mp_scraper is None:
                msg = str(exc) + getattr(exc, "msg", "") or ""
                if any(p in msg for p in ("proxy", "ERR_PROXY", "ERR_TUNNEL", "ERR_CONNECTION", "connect")):
                    logging.info("Воркер %d: прокси не работает → смена", worker_id)
                    continue
            raise
        finally:
            if mp_scraper:
                mp_scraper.close()

    return all_listings


def _run_track_clicks(args) -> None:
    """Режим отслеживания кликов: открыть страницу, пользователь кликает, логируем элемент."""
    if not has_display() and not args.headless:
        display = get_virtual_display()
        if not display.is_alive():
            raise DisplayNotFound()

    driver = MPDriver(
        base_url=MARTKPLAATS_BASE_URL,
        headless=args.headless,
        chromium_path=args.chromium_path,
        chromedriver_path=args.chromedriver_path,
        track_clicks=True,
        proxy=args.proxies[0] if args.proxies else None,
    )
    driver.quit()


def main():
    """Run the scraper."""
    signal.signal(signal.SIGTERM, handle_sigterm_interrupt)

    logging.basicConfig(
        format="%(asctime)s │ %(levelname)-5s │ %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    args = get_args()
    logging.debug("Аргументы: %s", args)

    # if no display is available and we aren't headless, create a virtual display
    display: Display | None = None
    if not has_display() and not args.headless:
        display = get_virtual_display()

        if not display.is_alive():
            raise DisplayNotFound()

    # Импорт функций БД (общая схема с ботом)
    try:
        from telegram_bot.database import (
            init_db,
            upsert_listings,
            load_listings_from_db,
            get_listings_count,
        )
        from telegram_bot.telegram_sender import send_listings_batch
    except ImportError:
        raise ImportError(
            "Для сохранения в БД нужен telegram_bot. Запускайте из корня проекта: python -m mpscraper"
        )

    listings_df = pd.DataFrame()
    item_ids: set[str] = set()

    listings_df, item_ids = load_listings_from_db(args.db_path)
    if len(listings_df) > 0:
        logging.info("БД: загружено %d объявлений", len(listings_df))
        if "item_id" in listings_df.columns:
            item_ids = set(str(x) for x in listings_df["item_id"].dropna())

        # recrawl listings with >= recrawl_hours since last crawl
        now_datetime = get_utc_now()
        if "crawled_timestamp" in listings_df.columns:
            for _, listing in listings_df.iterrows():
                item_id = str(listing.get("item_id", ""))
                crawled_ts = listing.get("crawled_timestamp")
                if not item_id or not crawled_ts or item_id not in item_ids:
                    continue
                try:
                    crawled_datetime = datetime.fromisoformat(str(crawled_ts).replace("Z", "+00:00"))
                    if diff_hours(crawled_datetime, now_datetime) >= args.recrawl_hours:
                        item_ids.discard(item_id)
                except (ValueError, TypeError):
                    pass

    if not os.path.isfile(args.chromium_path):
        raise NotFoundError(f"Chromium not found at: {args.chromium_path}")

    if args.chromedriver_path and not os.path.isfile(args.chromedriver_path):
        raise NotFoundError(f"ChromeDriver not found at: {args.chromedriver_path} ")

    if args.track_clicks:
        _run_track_clicks(args)
        return

    # Начальный скрапер: проверяем прокси по очереди, пока один не заработает
    logging.info("Запуск скрапера...")
    proxies_to_try = list(args.proxies) if args.proxies else [None]
    mp_scraper = None
    for proxy in proxies_to_try:
        try:
            if proxy:
                logging.info("Прокси: проверка...")
            mp_scraper = MpScraper(
                chromium_path=args.chromium_path,
                chromedriver_path=args.chromedriver_path,
                headless=args.headless,
                timeout_seconds=args.timeout_seconds,
                wait_seconds=args.wait_seconds,
                skip_cookies=args.skip_cookies,
                skip_count=args.skip_count,
                debug=args.debug,
                fast=False,
                block_css=args.block_css,
                proxy=proxy,
            )
            logging.info("Загрузка категорий...")
            parent_categories = mp_scraper.get_parent_categories()
            if proxy:
                logging.info("Прокси: OK ✓")
            break
        except ForbiddenError as fe:
            if proxy:
                logging.info("403 → смена прокси")
                if mp_scraper:
                    try:
                        mp_scraper.close()
                    except Exception:
                        pass
                mp_scraper = None
                continue
            raise
        except ProxyError as exc:
            logging.info("Прокси не работает → смена")
            if mp_scraper:
                try:
                    mp_scraper.close()
                except Exception:
                    pass
            mp_scraper = None
            continue
        except (WebDriverException, TimeoutException) as exc:
            if proxy:
                msg = str(exc) + getattr(exc, "msg", "") or ""
                if any(p in msg.lower() for p in ("proxy", "err_proxy", "err_tunnel", "err_connection", "timeout", "connect")):
                    logging.info("Прокси не работает → смена")
                    if mp_scraper:
                        try:
                            mp_scraper.close()
                        except Exception:
                            pass
                    mp_scraper = None
                    continue
            raise

    if mp_scraper is None:
        raise CategoriesError("Ни один прокси не сработал. Проверьте proxies.json")

    mp_scraper.close()
    logging.info("Категорий: %d", len(parent_categories))
    if len(parent_categories) == 0:
        raise CategoriesError("No parent categories found")

    parent_categories_list = list(parent_categories)

    # Лимит = сколько НОВЫХ объявлений скрапить (дополнительно к уже имеющимся в БД)
    if args.workers > 1:
        # Параллельная обработка: разбиваем категории по воркерам
        workers = min(args.workers, len(parent_categories_list))
        limit_per_worker = ((args.limit + workers - 1) // workers) if args.limit > 0 else 0
        chunks: list[list] = [[] for _ in range(workers)]
        for i, cat in enumerate(parent_categories_list):
            chunks[i % workers].append(cat)
        logging.info("Воркеров: %d", workers)

        all_listings: list[Listing] = []
        progress_queue: queue.Queue = queue.Queue()
        listings_lock = threading.Lock()
        listings_df_ref: list[pd.DataFrame] = [listings_df]
        workers_done = threading.Event()

        def _saver_thread_fn() -> None:
            batch: list[Listing] = []
            while True:
                try:
                    lst = progress_queue.get(timeout=0.5)
                    try:
                        send_listings_batch(lst)
                    except Exception as e:
                        logging.debug("Telegram рассылка: %s", e)
                    batch.extend(lst)
                    if len(batch) >= BATCH_SAVE_SIZE:
                        with listings_lock:
                            merged = pd.concat(
                                [listings_df_ref[0], _listings_to_df(batch)],
                                ignore_index=True,
                            )
                            merged = remove_duplicate_listings(merged)
                            upsert_listings(merged, args.db_path)
                            listings_df_ref[0] = merged
                        batch = []
                        logging.info("Сохранение: %d объявлений", len(listings_df_ref[0]))
                except queue.Empty:
                    if workers_done.is_set() and progress_queue.empty():
                        break
                except Exception as exc:
                    logging.exception("Сохранение пачки: %s", exc)
            if batch:
                with listings_lock:
                    merged = pd.concat(
                        [listings_df_ref[0], _listings_to_df(batch)],
                        ignore_index=True,
                    )
                    merged = remove_duplicate_listings(merged)
                    upsert_listings(merged, args.db_path)
                    listings_df_ref[0] = merged
                logging.info("Сохранение: %d объявлений", len(listings_df_ref[0]))

        saver_thread = threading.Thread(target=_saver_thread_fn, daemon=True)
        saver_thread.start()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _worker_scrape_categories,
                    w,
                    chunks[w],
                    limit_per_worker,
                    item_ids,
                    args,
                    progress_queue,
                ): w
                for w in range(workers)
            }
            for future in as_completed(futures):
                try:
                    listings = future.result()
                    all_listings.extend(listings)
                except Exception as exc:
                    logging.exception("Воркер %d: %s", futures[future], exc)

        workers_done.set()
        saver_thread.join(timeout=30)

        listings_df = listings_df_ref[0]
        if all_listings:
            listings_df = pd.concat([listings_df, _listings_to_df(all_listings)], ignore_index=True)
    else:
        # Последовательная обработка (1 воркер)
        proxies_to_try = list(args.proxies) if args.proxies else [None]
        for proxy in proxies_to_try:
            mp_scraper = None
            try:
                if proxy:
                    logging.info("Прокси...")
                mp_scraper = MpScraper(
                    chromium_path=args.chromium_path,
                    chromedriver_path=args.chromedriver_path,
                    headless=args.headless,
                    timeout_seconds=args.timeout_seconds,
                    wait_seconds=args.wait_seconds,
                    skip_cookies=args.skip_cookies,
                    skip_count=args.skip_count,
                    debug=args.debug,
                    fast=False,
                    block_css=args.block_css,
                    proxy=proxy,
                )
                remaining_limit = args.limit
                total = args.limit if args.limit > 0 else None
                stop = False
                save_state = {"df": listings_df, "last_saved": len(listings_df)}

                def _on_batch(batch: list[Listing]) -> None:
                    save_state["df"] = pd.concat(
                        [save_state["df"], _listings_to_df(batch)], ignore_index=True
                    )
                    try:
                        send_listings_batch(batch)
                    except Exception as e:
                        logging.debug("Telegram рассылка: %s", e)
                    if len(save_state["df"]) - save_state["last_saved"] >= BATCH_SAVE_SIZE:
                        save_state["df"] = remove_duplicate_listings(save_state["df"])
                        upsert_listings(save_state["df"], args.db_path)
                        save_state["last_saved"] = len(save_state["df"])
                        logging.info("Сохранение: %d объявлений", save_state["last_saved"])

                with tqdm(desc="Всего", total=total, position=0) as pbar:
                    for idx, parent_category in enumerate(parent_categories_list):
                        listings: list[Listing] = []
                        cat_slug = parent_category.url.rstrip("/").split("/")[-1]
                        logging.info("Категория %d/%d: %s", idx + 1, len(parent_categories_list), cat_slug)

                        if stop:
                            break

                        try:
                            listings = mp_scraper.get_listings(
                                parent_category=parent_category,
                                limit=remaining_limit,
                                existing_item_ids=item_ids,
                                on_batch=_on_batch,
                                max_age_hours=3.0,
                            )

                            listings_count = len(listings)
                            for listing in listings:
                                item_ids.add(listing.item_id)
                            if args.limit > 0:
                                remaining_limit = remaining_limit - listings_count
                                logging.info("  → %d новых, осталось %d", listings_count, remaining_limit)
                                if remaining_limit < 1:
                                    stop = True
                            else:
                                logging.info("  → %d новых", listings_count)
                            pbar.update(listings_count)
                        except CategoryStale as exc:
                            listings = exc.listings
                            logging.info(
                                "  → собрано %d, встретили объявление >3ч → следующая категория",
                                len(listings),
                            )
                            if len(listings) > 0:
                                for listing in listings:
                                    item_ids.add(listing.item_id)
                                if args.limit > 0:
                                    remaining_limit = remaining_limit - len(listings)
                                pbar.update(len(listings))
                                save_state["df"] = pd.concat(
                                    [save_state["df"], _listings_to_df(listings)],
                                    ignore_index=True,
                                )
                                save_state["df"] = remove_duplicate_listings(save_state["df"])
                                try:
                                    send_listings_batch(listings)
                                except Exception:
                                    pass
                                if len(save_state["df"]) - save_state["last_saved"] >= BATCH_SAVE_SIZE:
                                    upsert_listings(save_state["df"], args.db_path)
                                    save_state["last_saved"] = len(save_state["df"])
                            continue
                        except ListingsInterrupt as exc:
                            logging.info("Остановка")
                            listings = exc.listings
                            stop = True
                        except ListingsError as exc:
                            listings = exc.listings
                            stop = True
                        except ProxyError as exc:
                            logging.info("Прокси не работает → смена")
                            raise
                        except ForbiddenError as fe:
                            if proxy:
                                logging.info("403 → смена прокси")
                                raise ProxyError(proxy, fe.msg or "403") from fe
                            raise
                        except KeyboardInterrupt:
                            stop = True

                        if len(listings) > 0:
                            save_state["df"] = pd.concat(
                                [save_state["df"], _listings_to_df(listings)],
                                ignore_index=True,
                            )
                            save_state["df"] = remove_duplicate_listings(save_state["df"])
                            if len(save_state["df"]) - save_state["last_saved"] >= BATCH_SAVE_SIZE:
                                upsert_listings(save_state["df"], args.db_path)
                                save_state["last_saved"] = len(save_state["df"])
                                logging.info(
                                    "Промежуточное сохранение: %d объявлений в БД",
                                    save_state["last_saved"],
                                )

                listings_df = save_state["df"]
                break  # успех
            except ProxyError:
                continue
            except (WebDriverException, TimeoutException) as exc:
                if proxy and mp_scraper is None:
                    msg = str(exc) + getattr(exc, "msg", "") or ""
                    if any(p in msg for p in ("proxy", "ERR_PROXY", "ERR_TUNNEL", "ERR_CONNECTION", "connect")):
                        logging.info("Прокси не работает → смена")
                        if mp_scraper:
                            try:
                                mp_scraper.close()
                            except Exception:
                                pass
                        continue
                if mp_scraper:
                    try:
                        mp_scraper.close()
                    except Exception:
                        pass
                try:
                    listings_df = save_state["df"]
                except NameError:
                    pass
                raise
            finally:
                if mp_scraper:
                    mp_scraper.close()

    if len(listings_df.index) > 0:
        logging.info("Сохранение в БД: %s ✓", args.db_path)
        listings_df = remove_duplicate_listings(listings_df)
        upsert_listings(listings_df, args.db_path)
    else:
        logging.info("Нет новых объявлений")


if __name__ == "__main__":
    main()
