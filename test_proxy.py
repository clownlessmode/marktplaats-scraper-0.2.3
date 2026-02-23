#!/usr/bin/env python3
"""
Диагностика прокси: сначала все через requests, потом Selenium (httpbin → marktplaats).
Запуск: python test_proxy.py [--url URL]
Читает прокси из proxies.json. Без headless для визуальной проверки.

Важно: многие провайдеры (Bright Data, Oxylabs и т.д.) требуют whitelist доменов.
Прокси работают только для разрешённых сайтов. По умолчанию тестируем marktplaats.nl.
"""
import argparse
import json
import os
import re
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import requests
except ImportError:
    print("Ошибка: установите requests")
    sys.exit(1)

# По умолчанию — marktplaats (whitelist у провайдера). httpbin часто не в whitelist.
DEFAULT_TEST_URL = "https://www.marktplaats.nl/"


def load_proxies() -> list[str]:
    """Загрузить все прокси из proxies.json."""
    for base in (Path.cwd(), Path(__file__).resolve().parent):
        path = base / "proxies.json"
        if path.is_file():
            try:
                data = json.loads(path.read_text(encoding="utf-8").strip())
                if isinstance(data, list):
                    return [str(p).strip() for p in data if p]
            except (json.JSONDecodeError, OSError) as e:
                print(f"Ошибка загрузки proxies.json: {e}")
            break
    print("proxies.json не найден или пуст")
    sys.exit(1)


def parse_proxy(proxy_str: str) -> tuple[str, int, str, str]:
    """Парсит прокси → (host, port, user, pass)."""
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
    """Собрать URL для requests (http или socks5)."""
    host, port, user, pass_ = parse_proxy(proxy_str)
    scheme = "socks5" if socks5 else "http"
    if proxy_str.strip().startswith("socks"):
        return proxy_str.strip()
    if user and pass_:
        return f"{scheme}://{user}:{pass_}@{host}:{port}"
    return f"{scheme}://{host}:{port}"


def test_one_proxy(
    proxy: str, url: str, timeout: int = 30, socks5: bool = False
) -> tuple[str, bool, str]:
    """Проверить один прокси через requests. Возвращает (proxy, ok, msg)."""
    px = proxy_url(proxy, socks5=socks5)
    try:
        r = requests.get(
            url,
            proxies={"http": px, "https": px},
            timeout=timeout,
        )
        if r.status_code != 200:
            return (proxy, False, f"HTTP {r.status_code}")
        text = r.text.lower()
        if "origin" in text:
            m = re.search(r'"origin"\s*:\s*"([^"]+)"', r.text, re.I)
            return (proxy, True, m.group(1) if m else "OK")
        if "marktplaats" in text:
            return (proxy, True, "marktplaats OK")
        return (proxy, True, f"200, {len(r.text)} bytes")
    except Exception as e:
        return (proxy, False, str(e))


def js_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")


# === Парсинг аргументов ===
parser = argparse.ArgumentParser(description="Тест прокси из proxies.json")
parser.add_argument(
    "--url",
    default=DEFAULT_TEST_URL,
    help=f"URL для проверки (по умолч. {DEFAULT_TEST_URL} — whitelist провайдера)",
)
parser.add_argument("--timeout", type=int, default=30, help="Таймаут в секундах (по умолч. 30)")
parser.add_argument(
    "--socks5",
    action="store_true",
    help="Использовать SOCKS5 вместо HTTP (если прокси работают только по SOCKS5)",
)
args = parser.parse_args()

# Для SOCKS5 нужен PySocks: pip install requests[socks]
if args.socks5:
    try:
        import socks  # noqa: F401
    except ImportError:
        print("Для --socks5 установите: pip install requests[socks]")
        sys.exit(1)

# === 1. Загрузка прокси ===
proxies = load_proxies()
print(f"Загружено {len(proxies)} прокси из proxies.json")
print(f"Тест URL: {args.url}")
print(f"Протокол: {'SOCKS5' if args.socks5 else 'HTTP'}")

# === 2. Тест всех прокси через requests ===
print("\n=== Тест всех прокси через requests ===")
working: list[str] = []
dead: list[tuple[str, str]] = []

max_workers = min(20, len(proxies))
with ThreadPoolExecutor(max_workers=max_workers) as ex:
    futures = {
        ex.submit(test_one_proxy, p, args.url, args.timeout, args.socks5): p
        for p in proxies
    }
    for i, fut in enumerate(as_completed(futures), 1):
        proxy, ok, msg = fut.result()
        short = proxy.split(":")[0] if ":" in proxy else proxy[:20]
        if ok:
            working.append(proxy)
            print(f"  [{i}/{len(proxies)}] ✅ {short}... → {msg}")
        else:
            dead.append((proxy, msg))
            print(f"  [{i}/{len(proxies)}] ❌ {short}... → {msg}")

print(f"\nИтого: {len(working)} работают, {len(dead)} не работают")

if not working:
    print("\nНет рабочих прокси. Selenium не запускаем.")
    sys.exit(1)

# Берём первый рабочий для Selenium
PROXY = working[0]
HOST, PORT, USER, PASS = parse_proxy(PROXY)
print(f"\nДля Selenium используем: {HOST}:{PORT} ({'SOCKS5' if args.socks5 else 'HTTP'})")


print("\n=== Создание MV3-расширения ===")
PROXY_SCHEME = "socks5" if args.socks5 else "http"
u_esc = js_escape(USER)
p_esc = js_escape(PASS)

manifest = """{
  "manifest_version": 3,
  "name": "Proxy Auth",
  "version": "1.0",
  "permissions": ["proxy", "webRequest", "webRequestAuthProvider"],
  "host_permissions": ["<all_urls>"],
  "background": {"service_worker": "background.js"},
  "minimum_chrome_version": "120"
}"""

background = f"""
chrome.proxy.settings.set({{
  value: {{
    mode: "fixed_servers",
    rules: {{
      singleProxy: {{ scheme: "{PROXY_SCHEME}", host: "{HOST}", port: {PORT} }},
      bypassList: ["localhost", "127.0.0.1"]
    }}
  }},
  scope: "regular"
}});

chrome.webRequest.onAuthRequired.addListener(
  function(details, callback) {{
    callback({{
      authCredentials: {{
        username: "{u_esc}",
        password: "{p_esc}"
      }}
    }});
  }},
  {{ urls: ["<all_urls>"] }},
  ["asyncBlocking"]
);

console.log("Proxy extension loaded OK");
"""

ext_dir = tempfile.mkdtemp(prefix="proxy_test_")
with open(os.path.join(ext_dir, "manifest.json"), "w") as f:
    f.write(manifest)
with open(os.path.join(ext_dir, "background.js"), "w") as f:
    f.write(background)

print(f"Extension dir: {ext_dir}")


print("\n=== Тест Selenium ===")
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
except ImportError:
    print("Ошибка: установите selenium")
    sys.exit(1)

opts = Options()
opts.add_argument(f"--load-extension={ext_dir}")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-blink-features=AutomationControlled")

try:
    args = getattr(opts, "arguments", [])
    opts.arguments[:] = [
        a
        for a in args
        if a
        not in (
            "--disable-extensions",
            "--disable-component-extensions-with-background-pages",
        )
    ]
except (AttributeError, TypeError):
    pass

try:
    driver = webdriver.Chrome(options=opts)
except Exception as e:
    print(f"❌ Chrome не запустился: {e}")
    sys.exit(1)

time.sleep(2)

print("\nОткрываю httpbin.org/ip ...")
driver.set_page_load_timeout(30)
try:
    driver.get("https://httpbin.org/ip")
    time.sleep(3)
    src = driver.page_source
    print(f"Ответ:\n{src[:500]}")
    if "origin" in src:
        print("✅ Прокси работает в Selenium!")
    else:
        print("❌ 'origin' не найден")
except Exception as e:
    print(f"❌ Ошибка: {e}")

print("\nОткрываю marktplaats.nl ...")
try:
    driver.get("https://www.marktplaats.nl/")
    time.sleep(5)
    src = driver.page_source.lower()
    if "marktplaats" in src:
        print("✅ Marktplaats загрузился!")
    else:
        print(f"❌ Первые 500 символов:\n{src[:500]}")
except Exception as e:
    print(f"❌ Ошибка: {e}")

input("\nEnter для закрытия...")
driver.quit()
