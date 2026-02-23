#!/usr/bin/env python3
"""
Основной скрапер marktplaats: асинхронный (aiohttp) + BeautifulSoup.
Параллельные запросы страниц объявлений. Обходит категории, товары < 3ч в БД.

Использование:
  python fetch_listings.py --socks5
  python fetch_listings.py --socks5 --db-path bot.db
  pip install aiohttp aiohttp-socks
"""
# Загружаем .env до импорта telegram_bot (чтобы BOT_TOKEN и MP_TELEGRAM_CHAT_ID были доступны)
from pathlib import Path as _Path
_path = _Path(__file__).resolve().parent / ".env"
if _path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_path, override=True)
    except ImportError:
        import os
        for line in _path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v

import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import aiohttp
from bs4 import BeautifulSoup

# Импорт БД и Telegram из telegram_bot
sys.path.insert(0, str(Path(__file__).resolve().parent))
from telegram_bot.database import init_db, get_conn, get_workers_on_shift
from telegram_bot.telegram_sender import send_listing_to_next_worker, send_round_summary
from telegram_bot.email_sender import try_send_listing_email
from telegram_bot.config import TELEGRAM_CHAT_ID, ENVIRONMENT

MARTKPLAATS_BASE_URL = "https://marktplaats.nl"
# Сортировка: сначала новые (offeredSince:Altijd = все, sortBy=SORT_INDEX = по дате)
SORT_HASH = "#offeredSince:Altijd|sortBy:SORT_INDEX|sortOrder:DECREASING"

# Slug для /l/ отличается от /cp/ для некоторых категорий (cp -> l)
CP_TO_L_SLUG: dict[str, str] = {
    "auto-kopen": "auto-s",  # Auto's: /cp/91/auto-kopen/ -> /l/auto-s/
}

# Русские названия категорий (id -> RU)
CAT_ID_TO_RU: dict[int, str] = {
    1: "Антиквариат и искусство",
    31: "Аудио, ТВ и фото",
    91: "Автомобили",
    2600: "Автозапчасти",
    48: "Авто прочее",
    201: "Книги",
    289: "Караваны и кемпинг",
    1744: "CD и DVD",
    322: "Компьютеры и ПО",
    378: "Контакты и сообщения",
    1098: "Услуги и специалисты",
    395: "Животные и принадлежности",
    239: "Сделай сам и ремонт",
    445: "Велосипеды и мопеды",
    1099: "Хобби и досуг",
    504: "Дом и интерьер",
    1032: "Дома и комнаты",
    565: "Дети и малыши",
    621: "Одежда | Женская",
    1776: "Одежда | Мужская",
    678: "Мотоциклы",
    728: "Музыка и инструменты",
    1784: "Марки и монеты",
    1826: "Украшения, сумки и аксессуары",
    356: "Игровые и видеоигры",
    784: "Спорт и фитнес",
    820: "Телекоммуникация",
    1984: "Билеты",
    1847: "Сад и терраса",
    167: "Вакансии",
    856: "Отдых",
    895: "Коллекционирование",
    976: "Водный спорт и лодки",
    537: "Бытовая техника",
    1085: "Товары для бизнеса",
    428: "Разное",
}
DATA_ELEM_ID = "__NEXT_DATA__"
DEFAULT_CATEGORY_URL = f"{MARTKPLAATS_BASE_URL}/l/boeken/p/1/"
MAX_AGE_HOURS = 3.0
CONCURRENT_FETCHES = 8  # параллельных категорий (в тик N категорий + N товаров)


def load_proxy() -> str:
    """Первый прокси из proxies.json."""
    proxies = load_all_proxies()
    return proxies[0] if proxies else ""


def load_all_proxies() -> list[str]:
    """Все прокси из proxies.json."""
    for base in (Path.cwd(), Path(__file__).resolve().parent):
        path = base / "proxies.json"
        if path.is_file():
            try:
                data = json.loads(path.read_text(encoding="utf-8").strip())
                if isinstance(data, list) and data:
                    return [str(p).strip() for p in data if p]
            except (json.JSONDecodeError, OSError) as e:
                print(f"Ошибка: {e}")
            break
    return []


class ProxyRotator:
    """Ротация прокси при 403."""
    def __init__(self, proxies: list[str], socks5: bool = False):
        self.proxies = proxies
        self.socks5 = socks5
        self.idx = 0
        self._lock = asyncio.Lock()

    def current(self) -> str | None:
        return self.proxies[self.idx % len(self.proxies)] if self.proxies else None

    async def next_proxy(self) -> str | None:
        async with self._lock:
            self.idx += 1
            return self.current()

    def connector(self) -> aiohttp.BaseConnector:
        return _make_connector(self.current(), self.socks5)


def parse_proxy(proxy_str: str) -> tuple[str, int, str, str]:
    """(host, port, user, pass)."""
    from urllib.parse import urlparse

    s = proxy_str.strip()
    if s.startswith(("http://", "https://", "socks")):
        p = urlparse(s)
        return (
            p.hostname or "",
            int(p.port or 80),
            p.username or "",
            p.password or "",
        )
    parts = s.split(":", 3)
    if len(parts) == 4:
        return parts[0], int(parts[1]), parts[2], parts[3]
    if len(parts) >= 2:
        return parts[0], int(parts[1]), "", ""
    return s, 80, "", ""


def proxy_url(proxy_str: str, socks5: bool = False) -> str:
    """URL для aiohttp/requests proxy."""
    host, port, user, pass_ = parse_proxy(proxy_str)
    scheme = "socks5" if socks5 else "http"
    if proxy_str.strip().startswith("socks"):
        return proxy_str.strip()
    if user and pass_:
        return f"{scheme}://{user}:{pass_}@{host}:{port}"
    return f"{scheme}://{host}:{port}"


def _make_connector(proxy: str | None, socks5: bool):
    """Connector для aiohttp (с SOCKS5 при необходимости)."""
    import aiohttp
    if not proxy:
        return aiohttp.TCPConnector()
    try:
        from aiohttp_socks import ProxyConnector
        px = proxy_url(proxy, socks5=socks5)
        return ProxyConnector.from_url(px)
    except ImportError:
        return aiohttp.TCPConnector()


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}


async def fetch_page_async(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int = 30,
) -> str:
    """Загрузить HTML асинхронно."""
    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    async with session.get(url, headers=HEADERS, timeout=timeout_obj) as resp:
        resp.raise_for_status()
        return await resp.text()


def _is_403(e: Exception) -> bool:
    if isinstance(e, aiohttp.ClientResponseError):
        return e.status == 403
    return "403" in str(e) or "Forbidden" in str(e)


async def fetch_with_retry(
    url: str,
    rotator: ProxyRotator,
    timeout: int,
    max_retries: int = 5,
) -> str:
    """Загрузить с повтором при 403 (смена прокси)."""
    last_err: Exception | None = None
    for _ in range(max_retries):
        connector = rotator.connector()
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                html = await fetch_page_async(session, url, timeout)
                return html
        except Exception as e:
            last_err = e
            if _is_403(e) and rotator.proxies and len(rotator.proxies) > 1:
                await rotator.next_proxy()
                host, port = parse_proxy(rotator.current() or "")[:2]
                print(f"      ⚠ 403 → смена прокси на {host}:{port}")
            else:
                raise
    raise last_err or RuntimeError("max retries")


def extract_next_data(html: str):
    """Извлечь __NEXT_DATA__ из HTML (BeautifulSoup)."""
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", attrs={"id": DATA_ELEM_ID})
    if script and script.string:
        return json.loads(script.string)
    match = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>([^<]+)</script>', html)
    if match:
        return json.loads(match.group(1))
    return None


def parse_listings_from_next_data(data: dict) -> list[dict]:
    """Список объявлений из __NEXT_DATA__."""
    props = data.get("props", {}).get("pageProps", {})
    sr = props.get("searchRequestAndResponse", {})
    return sr.get("listings", [])


def get_categories(html: str) -> list[tuple[int, str, str]]:
    """
    Категории: сначала из HTML (hz-CategoryMenuBar-list), иначе из __CONFIG__.categoryLinks.
    Возвращает [(id, name, listing_url), ...]. URL с сортировкой по новизне.
    """
    result = _get_categories_from_html(html)
    if result:
        return result
    return _get_categories_from_config(html)


def _get_categories_from_html(html: str) -> list[tuple[int, str, str]]:
    """Парсинг категорий из hz-CategoryMenuBar-list (все категории в меню)."""
    soup = BeautifulSoup(html, "lxml")
    ul = soup.find("ul", class_="hz-CategoryMenuBar-list")
    if not ul:
        return []
    result = []
    for a in ul.find_all("a", class_="hz-CategoryMenuBarItem-link"):
        href = a.get("href", "")
        if not href.startswith("/cp/"):
            continue
        parts = href.rstrip("/").split("/")
        if len(parts) < 4:
            continue
        try:
            cat_id = int(parts[2])
        except (ValueError, IndexError):
            continue
        slug = parts[3]
        l_slug = CP_TO_L_SLUG.get(slug, slug)
        name = a.get_text(strip=True) or slug
        listing_url = f"{MARTKPLAATS_BASE_URL}/l/{l_slug}/p/1/{SORT_HASH}"
        result.append((cat_id, name, listing_url))
    return result


def _get_categories_from_config(html: str) -> list[tuple[int, str, str]]:
    """Категории из __CONFIG__.categoryLinks (fallback)."""
    config = extract_config(html)
    if not config:
        return []
    links = config.get("categoryLinks", [])
    result = []
    for link in links:
        url_path = link.get("url", "")
        if not url_path.startswith("/cp/"):
            continue
        parts = url_path.rstrip("/").split("/")
        slug = parts[-1] if len(parts) >= 4 else ""
        if not slug:
            continue
        cat_id = int(link.get("id", 0))
        name = link.get("name", slug)
        l_slug = CP_TO_L_SLUG.get(slug, slug)
        listing_url = f"{MARTKPLAATS_BASE_URL}/l/{l_slug}/p/1/{SORT_HASH}"
        result.append((cat_id, name, listing_url))
    return result


def details_to_db_row(details: dict, listing_url: str, parent_category_id: int) -> dict:
    """Конвертация details в строку для БД."""
    image_urls = details.get("image_urls") or []
    img_str = "|".join(str(u) for u in image_urls) if image_urls else ""
    attrs = details.get("attributes") or []
    attrs_json = json.dumps(attrs, ensure_ascii=False) if attrs else ""

    return {
        "item_id": details.get("item_id", ""),
        "seller_id": details.get("seller_id", "") or None,
        "parent_category_id": parent_category_id,
        "child_category_id": details.get("category_id"),
        "category_verticals": details.get("category_name", "") or None,
        "ad_type": details.get("ad_type", "") or None,
        "title": details.get("title", "") or None,
        "description": details.get("description", "") or None,
        "price_type": details.get("price_type", "") or None,
        "price_cents": int(details.get("price_cents", 0) or 0),
        "types": None,
        "services": None,
        "listing_url": listing_url or None,
        "image_urls": img_str or None,
        "city_name": details.get("city_name", "") or None,
        "country_code": details.get("country_code", "") or None,
        "listed_timestamp": details.get("listed_timestamp", "") or None,
        "crawled_timestamp": datetime.now(timezone.utc).isoformat(),
        "view_count": int(details.get("view_count", 0) or 0),
        "favorited_count": int(details.get("favorited_count", 0) or 0),
        "seller_name": details.get("seller_name", "") or None,
        "latitude": None,
        "longitude": None,
        "distance_meters": None,
        "country_name": details.get("country_name", "") or None,
        "priority_product": None,
        "traits": None,
        "category_specific_description": None,
        "reserved": 1 if details.get("reserved") else 0,
        "nap_available": 0,
        "urgency_feature_active": 0,
        "is_verified": 1 if details.get("seller_verified") else 0,
        "seller_website_url": None,
        "attributes_json": attrs_json or None,
    }


def save_listing_to_db(row: dict, db_path: str) -> None:
    """Вставить/обновить объявление в БД."""
    init_db(db_path)
    conn = get_conn(db_path)
    cols = list(row.keys())
    placeholders = ", ".join("?" * len(cols))
    cols_str = ", ".join(cols)
    conn.execute(
        f"INSERT OR REPLACE INTO listings ({cols_str}) VALUES ({placeholders})",
        [row.get(c) for c in cols],
    )
    conn.commit()
    conn.close()


def load_existing_item_ids(db_path: str) -> set[str]:
    """Загрузить item_id из БД."""
    if not Path(db_path).exists():
        return set()
    init_db(db_path)
    conn = get_conn(db_path)
    rows = conn.execute("SELECT item_id FROM listings").fetchall()
    conn.close()
    return {str(r[0]) for r in rows if r[0]}


def age_hours(since: str | None) -> float | None:
    """Часы с момента публикации. None если не удалось распарсить."""
    if not since:
        return None
    try:
        dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt
        return delta.total_seconds() / 3600
    except (ValueError, TypeError):
        return None


def extract_config(html: str) -> dict | None:
    """Извлечь window.__CONFIG__ из HTML (страница товара)."""
    idx = html.find("window.__CONFIG__")
    if idx == -1:
        return None
    start = html.find("=", idx) + 1
    if start <= 0:
        return None
    # Ищем начало JSON и парсим с учётом вложенных скобок
    depth = 0
    json_start = -1
    for i, c in enumerate(html[start:], start):
        if c == "{":
            if depth == 0:
                json_start = i
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[json_start : i + 1])
                except json.JSONDecodeError:
                    pass
                break
    return None


def parse_listing_details(config: dict) -> dict | None:
    """Все доступные данные товара из __CONFIG__.listing."""
    listing = config.get("listing")
    if not listing:
        return None
    stats = listing.get("stats", {})
    price_info = listing.get("priceInfo", {})
    # На странице товара: seller (не sellerInformation), seller.location
    seller = listing.get("seller") or listing.get("sellerInformation") or listing.get("sellerInfo") or {}
    seller_loc = seller.get("location") or {}
    # category как объект {id, name, fullName}
    category = listing.get("category") or {}

    # Картинки: gallery.imageUrls или gallery.media.images[].base
    image_urls = []
    gallery = listing.get("gallery") or {}
    urls = gallery.get("imageUrls") or []
    for u in urls:
        if isinstance(u, str) and u:
            image_urls.append(u if u.startswith("http") else "https:" + u)
    if not image_urls:
        for img in gallery.get("media", {}).get("images", []) or []:
            base = img.get("base", "")
            if base:
                image_urls.append(base if base.startswith("http") else "https:" + base)
    if not image_urls:
        for p in listing.get("pictures") or listing.get("images") or []:
            if isinstance(p, dict):
                u = p.get("extraExtraLargeUrl") or p.get("largeUrl") or p.get("url")
                if u:
                    image_urls.append(u)

    return {
        "item_id": listing.get("itemId", ""),
        "title": listing.get("title", ""),
        "description": (listing.get("description") or "")[:10000],
        "ad_type": listing.get("adType", ""),
        "price_type": price_info.get("priceType", ""),
        "price_cents": price_info.get("priceCents", 0),
        "listed_timestamp": stats.get("since"),
        "view_count": stats.get("viewCount", 0),
        "favorited_count": stats.get("favoritedCount", 0),
        "seller_id": str(seller.get("id") or seller.get("sellerId", "")),
        "seller_name": seller.get("name") or seller.get("sellerName", ""),
        "seller_verified": seller.get("isVerified", False),
        "seller_member_since": seller.get("activeSinceDiff") or seller.get("memberSince", ""),
        "city_name": seller_loc.get("cityName", ""),
        "country_code": seller_loc.get("countryAbbreviation", ""),
        "country_name": seller_loc.get("countryName", ""),
        "image_count": len(image_urls),
        "image_urls": image_urls,
        "category_id": category.get("id") or listing.get("categoryId"),
        "category_name": category.get("name") or category.get("fullName") or listing.get("categoryName", ""),
        "reserved": listing.get("reserved", False),
        "attributes": listing.get("attributes", []),
    }


async def _fetch_detail_with_retry(
    item: dict,
    rotator: ProxyRotator,
    timeout: int,
) -> tuple[dict | None, str, str]:
    """Загрузить страницу объявления с повтором при 403."""
    vip_url = item.get("vipUrl")
    if not vip_url:
        return None, "", "no vipUrl"
    listing_url = f"{MARTKPLAATS_BASE_URL}{vip_url}"
    try:
        html = await fetch_with_retry(listing_url, rotator, timeout)
    except Exception as e:
        return None, listing_url, str(e)
    config = extract_config(html)
    if not config:
        return None, listing_url, "no config"
    details = parse_listing_details(config)
    if not details:
        return None, listing_url, "no details"
    return details, listing_url, ""


async def _worker_category(
    queue: asyncio.Queue,
    rotator: ProxyRotator,
    sem: asyncio.Semaphore,
    timeout: int,
    max_age_hours: float,
    remaining_limit: list[int],
    existing_item_ids: set[str],
    db_path: str,
    results: dict,
) -> None:
    """
    Воркер: обрабатывает категории из очереди.
    concurrent=N: N воркеров, в тик до N запросов категорий + до N запросов товаров.
    """
    while True:
        try:
            task = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        idx, cat_id, cat_name, cat_url = task
        if remaining_limit and remaining_limit[0] <= 0:
            queue.put_nowait(task)
            break
        cat_display = CAT_ID_TO_RU.get(cat_id, cat_name)
        t0 = time.perf_counter()
        saved = 0
        try:
            async with sem:
                html = await fetch_with_retry(cat_url, rotator, timeout)
            data = extract_next_data(html)
            if not data:
                results[idx] = (0, time.perf_counter() - t0, False)
                continue
            all_listings = parse_listings_from_next_data(data)
            if not all_listings:
                results[idx] = (0, time.perf_counter() - t0, False)
                continue

            to_fetch: list[dict] = []
            limit_val = remaining_limit[0] if remaining_limit else 0
            for item in all_listings:
                if limit_val > 0 and saved + len(to_fetch) >= limit_val:
                    break
                if item.get("itemId", "") in existing_item_ids:
                    continue
                if not item.get("vipUrl"):
                    continue
                to_fetch.append(item)

            stale = False
            for item in to_fetch:
                if remaining_limit and remaining_limit[0] <= 0:
                    break
                async with sem:
                    details, listing_url, err = await _fetch_detail_with_retry(item, rotator, timeout)
                if details is None:
                    if err and "403" not in str(err):
                        title = item.get("title", "")[:50]
                        print(f"      ⚠ [{cat_display}] «{title}» — {err}")
                    continue
                title = details.get("title", "")[:50]
                hours = age_hours(details.get("listed_timestamp"))
                if hours is not None and hours >= max_age_hours:
                    print(f"      → [{cat_display}] «{title}» — {hours:.1f} ч (>3ч)")
                    stale = True
                    break
                item_id = details.get("item_id", "")
                row = details_to_db_row(details, listing_url, cat_id)
                save_listing_to_db(row, db_path)
                existing_item_ids.add(item_id)
                saved += 1
                if remaining_limit and remaining_limit[0] > 0:
                    remaining_limit[0] -= 1
                row["category_ru"] = CAT_ID_TO_RU.get(cat_id, "")
                ns = SimpleNamespace(**row)
                ok, worker_id = send_listing_to_next_worker(ns, db_path)
                print(f"      📤 Telegram: {'OK' if ok else 'пропуск (нет воркеров на смене)'} «{title[:40]}»", flush=True)
                try:
                    email_ok, recipient = try_send_listing_email(db_path, ns, worker_id)
                    if email_ok and recipient:
                        print(f"      📧 Email: отправлено на {recipient} «{title[:40]}»", flush=True)
                    elif email_ok:
                        print(f"      📧 Email: OK «{title[:40]}»", flush=True)
                    elif ENVIRONMENT == "dev":
                        print(f"      📧 Email: пропуск (нет шаблона/почт) «{title[:40]}»", flush=True)
                except Exception as ex:
                    print(f"      ⚠ Email: {ex}", flush=True)
                if hours is not None:
                    print(f"      ✓ [{cat_display}] «{title}» — {hours:.1f} ч")
                else:
                    print(f"      ✓ [{cat_display}] «{title}»")

            results[idx] = (saved, time.perf_counter() - t0, stale)
        except Exception as e:
            print(f"      ❌ [{cat_display}] {e}")
            results[idx] = (0, time.perf_counter() - t0, False)


async def run_one_round_async(
    proxies: list[str],
    socks5: bool,
    timeout: int,
    max_age_hours: float,
    limit: int,
    db_path: str,
    existing_item_ids: set[str],
    concurrent: int = CONCURRENT_FETCHES,
) -> tuple[int, float, list[float]]:
    """
    Один проход: concurrent категорий параллельно.
    В тик: до N запросов категорий + до N запросов товаров (по 1 на верхний товар из каждой).
    """
    t_round_start = time.perf_counter()
    rotator = ProxyRotator(proxies, socks5)
    sem = asyncio.Semaphore(concurrent)

    try:
        t0 = time.perf_counter()
        main_html = await fetch_with_retry(MARTKPLAATS_BASE_URL + "/", rotator, timeout)
        print(f"Главная страница: {time.perf_counter() - t0:.1f} с")
    except Exception as e:
        print(f"❌ Ошибка загрузки главной: {e}")
        return 0, time.perf_counter() - t_round_start, []

    categories = get_categories(main_html)
    if not categories:
        print("❌ Категории не найдены")
        return 0, time.perf_counter() - t_round_start, []

    queue: asyncio.Queue = asyncio.Queue()
    for idx, (cat_id, cat_name, cat_url) in enumerate(categories):
        queue.put_nowait((idx + 1, cat_id, cat_name, cat_url))

    results: dict[int, tuple[int, float, bool]] = {}
    remaining_limit = [limit] if limit > 0 else []
    workers = [
        asyncio.create_task(_worker_category(
            queue, rotator, sem, timeout, max_age_hours, remaining_limit,
            existing_item_ids, db_path, results,
        ))
        for _ in range(min(concurrent, len(categories)))
    ]
    await asyncio.gather(*workers)

    total_saved = 0
    category_times: list[float] = []
    for idx in range(1, len(categories) + 1):
        if idx in results:
            saved, cat_sec, stale = results[idx]
            total_saved += saved
            category_times.append(cat_sec)
            cat_display = CAT_ID_TO_RU.get(categories[idx - 1][0], categories[idx - 1][1])
            print(f"[{idx}/{len(categories)}] {cat_display} — {saved} сохранено, ⏱ {cat_sec:.1f} с" + (" (устарела)" if stale else ""))

    total_sec = time.perf_counter() - t_round_start
    return total_saved, total_sec, category_times


def main() -> int:
    parser = argparse.ArgumentParser(description="Скрапер marktplaats: категории, товары < 3ч, БД, бесконечный цикл")
    parser.add_argument("--socks5", action="store_true", help="Использовать SOCKS5")
    parser.add_argument("--no-proxy", action="store_true", help="Без прокси")
    parser.add_argument("--db-path", default="bot.db", help="Путь к SQLite БД")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--limit", type=int, default=0, help="Макс. товаров за раунд (0 = без лимита)")
    parser.add_argument("--max-age", type=float, default=MAX_AGE_HOURS, help="Макс. возраст в часах")
    parser.add_argument("--pause-minutes", type=int, default=10, help="Пауза в мин. когда раунд без новых")
    parser.add_argument("--once", action="store_true", help="Один раунд и выход (без бесконечного цикла)")
    parser.add_argument("--concurrent", type=int, default=CONCURRENT_FETCHES,
                        help="Параллельных категорий (в тик: N запросов категорий + N запросов товаров)")
    args = parser.parse_args()

    if args.socks5:
        try:
            import aiohttp_socks  # noqa: F401
        except ImportError:
            print("Для --socks5: pip install aiohttp-socks")
            return 1

    proxies: list[str] = []
    if not args.no_proxy:
        proxies = load_all_proxies()
        if not proxies:
            print("proxies.json не найден или пуст")
            return 1
    else:
        proxies = [""]  # без прокси — один "пустой" прокси
    print(f"Прокси: {'нет' if args.no_proxy else f'{len(proxies)} шт.'} ({'SOCKS5' if args.socks5 else 'HTTP'})")
    print(f"БД: {args.db_path}")
    print(f"Макс. возраст: {args.max_age} ч")
    print(f"Пауза: {args.pause_minutes} мин.")
    print(f"Параллельно: {args.concurrent} запросов")
    _tok = "есть" if __import__("os").environ.get("CLIENT_BOT_TOKEN") or __import__("os").environ.get("BOT_TOKEN") else "НЕТ"
    print(f"Telegram: воркеры на смене (round-robin), token={_tok}", flush=True)
    print()

    init_db(args.db_path)

    round_num = 0
    while True:
        round_num += 1
        print(f"\n{'='*60}")
        print(f"Раунд {round_num}")
        print(f"{'='*60}")

        workers_on_shift = get_workers_on_shift(args.db_path)
        if not workers_on_shift:
            print("⏭ Нет воркеров на смене — пропуск раунда (без запросов к прокси)")
            if args.once:
                break
            pause_sec = args.pause_minutes * 60
            print(f"\nПауза {args.pause_minutes} мин. до следующей проверки...")
            time.sleep(pause_sec)
            continue

        existing_item_ids = load_existing_item_ids(args.db_path)
        print(f"В БД: {len(existing_item_ids)} объявлений")
        print(f"Воркеров на смене: {len(workers_on_shift)}")

        saved, total_sec, cat_times = asyncio.run(run_one_round_async(
            proxies, args.socks5, args.timeout, args.max_age, args.limit,
            args.db_path, existing_item_ids, args.concurrent,
        ))

        avg_s = total_sec / len(cat_times) if cat_times else 0
        print(f"\nРаунд {round_num}: сохранено {saved} новых | {total_sec:.1f} с всего | "
              f"{avg_s:.1f} с/категория в среднем")

        if ENVIRONMENT == "dev" and TELEGRAM_CHAT_ID:
            send_round_summary(round_num, saved, total_sec, avg_s, len(existing_item_ids))

        if args.once:
            break

        pause_sec = args.pause_minutes * 60
        print(f"\nПауза {args.pause_minutes} мин. до следующего раунда...")
        time.sleep(pause_sec)

    return 0


if __name__ == "__main__":
    sys.exit(main())
