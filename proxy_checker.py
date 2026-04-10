#!/usr/bin/env python3
"""
Проверка прокси перед запуском скрапера.
Читает proxies.json, проверяет каждый через requests, перезаписывает только рабочие.

Это не гарантирует успех в Chrome/Selenium: браузер грузит страницу иначе (дольше).
Скрапер выставляет отдельный таймаут первой загрузки страницы при прокси (см. MPDriver).
"""
import json
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

try:
    import requests
except ModuleNotFoundError:
    exe = sys.executable
    print("Нет пакета requests для ЭТОГО интерпретатора:", exe, sep="\n  ")
    print("Установка в то же окружение:")
    print(f"  {exe} -m pip install requests")
    print("(Если (venv) активен, а ошибка остаётся — `python3` указывает не на venv; используйте ./venv/bin/python3)")
    sys.exit(1)

CONFIG_FILE = "proxy_checker_config.json"
DEFAULT_CONFIG = {
    "thread": 20,
    "timeout": 15,
    "max_ms": 20000,
    "host": "https://httpbin.org/ip",
    "proxies_file": "proxies.json",
}


def load_config() -> dict:
    """Загрузить конфиг."""
    for base in (Path.cwd(), Path(__file__).resolve().parent):
        path = base / CONFIG_FILE
        if path.is_file():
            with open(path, encoding="utf-8") as f:
                return json.load(f)
    return DEFAULT_CONFIG.copy()


def load_proxies(path: Path) -> list[str]:
    """Загрузить прокси из JSON."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return [str(p).strip() for p in data if p]


def save_proxies(path: Path, proxies: list[str]) -> None:
    """Сохранить прокси в JSON."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(proxies, f, indent=2, ensure_ascii=False)


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


def to_requests_proxy_url(proxy: str) -> str:
    """
    URL для requests.proxies.
    Поддержка: http(s)://..., socks5://..., host:port:user:pass, host:port.
    Нельзя делать http:// + целиком host:port:user:pass — urllib3 не парсит.
    """
    s = proxy.strip()
    if not s:
        return s
    if "://" in s:
        return s
    parts = s.split(":", 3)
    if len(parts) == 4:
        host, port, user, password = parts
        u = quote(user, safe="")
        p = quote(password, safe="")
        return f"http://{u}:{p}@{host}:{port}"
    if len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}"
    return f"http://{s}"


def check_proxy(proxy: str, config: dict) -> tuple[str, bool, str]:
    """
    Проверить один прокси. Возвращает (proxy, ok, info).
    Только marktplaats — без ipinfo (часто блокирует прокси).
    """
    proxy = proxy.strip()
    if not proxy:
        return (proxy, False, "empty")
    proxy_url = to_requests_proxy_url(proxy)
    proxies = {"https": proxy_url, "http": proxy_url}
    try:
        start = time.perf_counter()
        r = requests.get(
            config["host"],
            proxies=proxies,
            timeout=config["timeout"],
            headers=HEADERS,
        )
        elapsed_ms = round((time.perf_counter() - start) * 1000)

        if r.status_code != 200:
            return (proxy, False, f"host {r.status_code}")
        # httpbin.org/ip возвращает JSON; marktplaats может блокировать requests
        if config["host"].find("marktplaats") >= 0 and "marktplaats" not in r.text.lower():
            return (proxy, False, "no marktplaats in response")
        if elapsed_ms > config["max_ms"]:
            return (proxy, False, f"slow {elapsed_ms}ms")
        return (proxy, True, f"{elapsed_ms}ms")
    except requests.exceptions.Timeout:
        return (proxy, False, "timeout")
    except requests.exceptions.ProxyError:
        return (proxy, False, "proxy error")
    except requests.exceptions.RequestException as e:
        return (proxy, False, str(e)[:50])
    except Exception as e:
        return (proxy, False, str(e)[:50])


def run_checker(proxies_path: Path | None = None, config: dict | None = None) -> list[str]:
    """
    Запустить проверку. Возвращает список рабочих прокси.
    Перезаписывает proxies.json только рабочими.
    """
    config = config or load_config()
    path = proxies_path or Path(config["proxies_file"])
    if not path.is_file():
        print(f"Error: {path} not found")
        return []

    proxies = load_proxies(path)
    if not proxies:
        print("No proxies to check")
        return []

    print(f"Checking {len(proxies)} proxies (threads={config['thread']}, timeout={config['timeout']}s, max_ms={config['max_ms']})...")
    working: list[str] = []
    checked = 0

    with ThreadPoolExecutor(max_workers=config["thread"]) as executor:
        futures = {executor.submit(check_proxy, p, config): p for p in proxies}
        for future in as_completed(futures):
            proxy, ok, info = future.result()
            checked += 1
            if ok:
                print(f"  OK   {proxy} | {info}")
                working.append(proxy)
            else:
                print(f"  FAIL {proxy} | {info}")
            if checked % 50 == 0:
                print(f"  ... {checked}/{len(proxies)}")

    print(f"\nWorking: {len(working)}/{len(proxies)}")
    if working:
        save_proxies(path, working)
        print(f"Saved to {path}")
    else:
        print("Все провалились — proxies.json не трогаем (запусти без --check-proxies)")
    return working


def main() -> None:
    """CLI entry."""
    config = load_config()
    working = run_checker(config=config)
    sys.exit(0 if working else 1)


if __name__ == "__main__":
    main()
