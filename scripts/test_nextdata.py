#!/usr/bin/env python3
"""Тестовый запуск: загрузить страницу и сохранить __NEXT_DATA__ для просмотра.

Использование:
  python scripts/test_nextdata.py              # через requests (без браузера)
  python scripts/test_nextdata.py --selenium   # через Selenium/Chrome
  python scripts/test_nextdata.py "https://..."  # свой URL
"""
import json
import re
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup

MARTKPLAATS_BASE_URL = "https://marktplaats.nl"
DATA_ELEM_ID = "__NEXT_DATA__"


def fetch_with_requests(url: str) -> str:
    """Загрузить страницу через requests (без JS, но __NEXT_DATA__ в HTML есть)."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def extract_next_data(html: str):
    """Извлечь __NEXT_DATA__ из HTML."""
    # Ищем <script id="__NEXT_DATA__" type="application/json">...</script>
    match = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>([^<]+)</script>', html)
    if match:
        return json.loads(match.group(1))
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", attrs={"id": DATA_ELEM_ID})
    if script and script.string:
        return json.loads(script.string)
    return None


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    use_selenium = "--selenium" in sys.argv

    url = args[0] if args else f"{MARTKPLAATS_BASE_URL}/l/boeken/p/1/#sortBy:SORT_INDEX|sortOrder:DECREASING"
    # Убираем хэш для requests
    fetch_url = url.split("#")[0] if "#" in url else url

    print(f"Загружаю: {fetch_url}")

    if use_selenium:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from mpscraper.driver import MPDriver
        from mpscraper.mpscraper import DATA_ELEM_ID as _  # noqa
        from bs4 import Tag

        driver = MPDriver(
            base_url=MARTKPLAATS_BASE_URL,
            headless=True,
            skip_cookies=False,
            proxy=None,
        )
        try:
            driver.get(fetch_url)
            html = driver.page_source
        finally:
            driver.quit()
    else:
        print("(через requests, без браузера)")
        html = fetch_with_requests(fetch_url)

    data = extract_next_data(html)
    if not data:
        print("ОШИБКА: __NEXT_DATA__ не найден")
        debug_path = Path.cwd() / "debug_test_nextdata.html"
        debug_path.write_text(html[:50000], encoding="utf-8")
        print(f"HTML (первые 50k) сохранён в {debug_path}")
        return 1

    out_path = Path.cwd() / "test_nextdata_output.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ __NEXT_DATA__ сохранён в: {out_path}")

    props = data.get("props", {}).get("pageProps", {})
    print("\n--- Структура pageProps ---")
    for key in sorted(props.keys()):
        val = props[key]
        if isinstance(val, list):
            print(f"  {key}: list[{len(val)}]")
        elif isinstance(val, dict):
            subkeys = list(val.keys())[:8]
            print(f"  {key}: dict (ключи: {subkeys}{'...' if len(val) > 8 else ''})")
        else:
            print(f"  {key}: {type(val).__name__}")

    if "searchRequestAndResponse" in props:
        sr = props["searchRequestAndResponse"]
        listings = sr.get("listings", [])
        print(f"\n--- Объявлений на странице: {len(listings)} ---")
        if listings:
            first = listings[0]
            print(f"  Первое: itemId={first.get('itemId')}, title={first.get('title', '')[:50]}...")
            print(f"  Все ключи объявления ({len(first)}):")
            for k in first.keys():
                print(f"    - {k}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
