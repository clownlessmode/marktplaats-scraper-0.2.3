"""
Chrome-расширение Manifest V3 для прокси с авторизацией.
Chrome 120+ поддерживает onAuthRequired с asyncBlocking.
"""
import os
import tempfile


def create_proxy_extension(
    host: str, port: int, user: str, password: str, scheme: str = "http"
) -> str:
    """
    Создаёт MV3-расширение для прокси с авторизацией.
    Chrome 120+ поддерживает onAuthRequired с asyncBlocking.
    scheme: "http" или "socks5".
    Возвращает путь к папке расширения.
    """
    scheme = "socks5" if scheme and str(scheme).lower() == "socks5" else "http"

    manifest = """{
  "manifest_version": 3,
  "name": "Proxy Auth",
  "version": "1.0",
  "permissions": [
    "proxy",
    "webRequest",
    "webRequestAuthProvider"
  ],
  "host_permissions": [
    "<all_urls>"
  ],
  "background": {
    "service_worker": "background.js"
  },
  "minimum_chrome_version": "120"
}"""

    def js_escape(s: str) -> str:
        return s.replace("\\", "\\\\").replace('"', '\\"').replace("'", "\\'")

    h = js_escape(host)
    u = js_escape(user)
    p = js_escape(password)

    background = f"""
chrome.proxy.settings.set({{
  value: {{
    mode: "fixed_servers",
    rules: {{
      singleProxy: {{
        scheme: "{scheme}",
        host: "{h}",
        port: {int(port)}
      }},
      bypassList: ["localhost", "127.0.0.1"]
    }}
  }},
  scope: "regular"
}});

chrome.webRequest.onAuthRequired.addListener(
  function(details, callback) {{
    callback({{
      authCredentials: {{
        username: "{u}",
        password: "{p}"
      }}
    }});
  }},
  {{ urls: ["<all_urls>"] }},
  ["asyncBlocking"]
);
"""

    ext_dir = tempfile.mkdtemp(prefix="proxy_ext_")
    with open(os.path.join(ext_dir, "manifest.json"), "w", encoding="utf-8") as f:
        f.write(manifest)
    with open(os.path.join(ext_dir, "background.js"), "w", encoding="utf-8") as f:
        f.write(background)

    return ext_dir
