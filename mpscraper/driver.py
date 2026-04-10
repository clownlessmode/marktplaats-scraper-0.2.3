import os
import time
from selenium.webdriver.remote.webelement import WebElement
from fake_http_header import FakeHttpHeader
from selenium.webdriver import ChromeOptions, ChromeService
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import logging
from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from bs4 import BeautifulSoup as Soup
from bs4 import Tag

from .exceptions import ElementNotFound, MPError, ForbiddenError, ProxyError
from .proxy_ext import create_proxy_extension

ACCEPT_COOKIES_TIMEOUT_SECONDS = 30

# Cookie consent selectors (Marktplaats / SourcePoint)
# Работают и в основном документе (#notice), и внутри consent iframe
COOKIE_SELECTORS = [
    # SourcePoint / consent iframe
    (By.CSS_SELECTOR, "button[aria-label='Accepteren']"),
    (By.CSS_SELECTOR, "[aria-label='Accepteren']"),
    (By.CSS_SELECTOR, ".message-button.primary"),
    (By.CSS_SELECTOR, ".sp_choice_type_11"),  # SourcePoint accept all
    (By.XPATH, "//button[contains(., 'Accepteren')]"),
    (By.XPATH, "//*[@role='button' and contains(., 'Accepteren')]"),
    (By.XPATH, "//button[contains(., 'Accept')]"),
    # Inside #notice modal (legacy)
    (By.CSS_SELECTOR, "#notice button[aria-label='Accepteren']"),
    (By.CSS_SELECTOR, "#notice .message-button.primary"),
    (By.XPATH, "//*[@id='notice']//button[@aria-label='Accepteren']"),
    # Fallbacks
    (By.XPATH, "//a[contains(., 'Doorgaan zonder te accepteren')]"),
    (By.XPATH, "//button[contains(., 'Doorgaan')]"),
]
MARKTPLAATS_403_URL = "https://www.marktplaats.nl/403/"
# Consent iframe (SourcePoint) - id sp_message_iframe_* или src consent.marktplaats.nl
CONSENT_IFRAME_SELECTORS = [
    (By.CSS_SELECTOR, "iframe[id^='sp_message_iframe_']"),
    (By.CSS_SELECTOR, "iframe[src*='consent.marktplaats.nl']"),
]


def _parse_proxy(proxy_str: str) -> dict:
    """
    Парсит прокси из строки.
    Форматы: host:port:user:pass | host:port | http://user:pass@host:port
    """
    from urllib.parse import urlparse

    s = proxy_str.strip()

    if s.startswith(("http://", "https://", "socks")):
        p = urlparse(s)
        return {
            "host": p.hostname or "",
            "port": int(p.port or 80),
            "user": p.username,
            "pass": p.password,
        }

    parts = s.split(":", 3)
    if len(parts) == 4:
        return {
            "host": parts[0],
            "port": int(parts[1]),
            "user": parts[2],
            "pass": parts[3],
        }
    if len(parts) >= 2:
        return {
            "host": parts[0],
            "port": int(parts[1]),
            "user": None,
            "pass": None,
        }

    return {"host": s, "port": 80, "user": None, "pass": None}


def _create_driver(
    chrome_options: ChromeOptions,
    chromedriver_path: str | None,
    proxy: str | None,
):
    """
    Создать Chrome WebDriver.
    Прокси с авторизацией → через MV3-расширение (onAuthRequired).
    MP_PROXY_TYPE=socks5 для SOCKS5 (Chrome не поддерживает HTTP с auth нативно).
    """
    from selenium.webdriver.chrome.webdriver import WebDriver

    proxy_val = proxy.strip() if proxy else None
    ext_dir = None
    scheme = "socks5" if os.environ.get("MP_PROXY_TYPE", "").lower() == "socks5" else "http"

    if proxy_val:
        p = _parse_proxy(proxy_val)

        if p["user"] and p["pass"]:
            ext_dir = create_proxy_extension(
                p["host"], p["port"], p["user"], p["pass"], scheme=scheme
            )
            try:
                args = getattr(chrome_options, "arguments", [])
                chrome_options.arguments[:] = [
                    a for a in args
                    if a not in (
                        "--disable-extensions",
                        "--disable-component-extensions-with-background-pages",
                    )
                ]
            except (AttributeError, TypeError):
                pass
            chrome_options.add_argument(f"--load-extension={ext_dir}")
        else:
            chrome_options.add_argument(
                f'--proxy-server={scheme}://{p["host"]}:{p["port"]}'
            )

    service = (
        ChromeService(executable_path=chromedriver_path)
        if chromedriver_path
        else ChromeService()
    )
    driver = WebDriver(options=chrome_options, service=service)

    if ext_dir:
        time.sleep(1.5)

    return driver


class MPDriver:
    def __init__(
        self,
        base_url: str,
        chromedriver_path: str | None = None,
        chromium_path: str | None = None,
        headless: bool = False,
        skip_cookies: bool = False,
        track_clicks: bool = False,
        proxy: str | None = None,
        block_css: bool = False,
    ) -> None:
        chrome_options = ChromeOptions()
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = "eager"  # DOMContentLoaded, не ждём полной загрузки

        fake_header = FakeHttpHeader(domain_code="nl")
        headers_dict = fake_header.as_header_dict()
        if headers_dict.get("User-Agent"):
            chrome_options.add_argument(f"--user-agent={headers_dict['User-Agent']}")
        extra_headers = {k: v for k, v in headers_dict.items() if k != "User-Agent" and v}
        self.__extra_headers = extra_headers

        if headless:
            chrome_options.add_argument("--headless=new")

        self._driver = _create_driver(chrome_options, chromedriver_path, proxy)

        if self.__extra_headers and hasattr(self._driver, "execute_cdp_cmd"):
            try:
                self._driver.execute_cdp_cmd(
                    "Network.setExtraHTTPHeaders",
                    {"headers": self.__extra_headers},
                )
            except Exception as e:
                logging.warning("Не удалось установить заголовки CDP: %s", e)

        if block_css and hasattr(self._driver, "execute_cdp_cmd"):
            try:
                self._driver.execute_cdp_cmd("Network.enable", {})
                self._driver.execute_cdp_cmd(
                    "Network.setBlockedURLs",
                    {"urls": ["*.css", "*stylesheet*", "*styles*.css*"]},
                )
                logging.info("Блокировка CSS включена")
            except Exception as e:
                logging.warning("Не удалось заблокировать CSS (CDP): %s", e)

        # делегируем к _driver
        for attr in ("get", "page_source", "find_elements", "find_element", "quit", "refresh",
                     "execute_script", "switch_to", "current_url", "set_page_load_timeout"):
            setattr(self, attr, getattr(self._driver, attr))

        self.__skip_cookies = skip_cookies
        self.__track_clicks = track_clicks
        self.__proxy = proxy
        self.set_page_load_timeout(5 if proxy else 20)
        self.__accept_cookies(url=base_url)

    def __accept_cookies(self, url: str) -> None:
        """Accept or dismiss the cookies banner."""
        try:
            logging.debug("Открываю: %s", url.split("/")[-1] or url[:50])
            self.get(url)
            if self.__proxy:
                self._validate_proxy_page()
            if self.__track_clicks:
                self._track_clicks_mode(url)
                return
            if self.__skip_cookies:
                logging.info(">>> Примите куки вручную в браузере, затем нажмите Enter <<<")
                input()
                return
            self._do_accept_cookies()
        except (TimeoutException, WebDriverException) as e:
            if self.__proxy:
                msg = str(e) + getattr(e, "msg", "") or ""
                if any(p in msg.lower() for p in ("proxy", "err_proxy", "err_tunnel", "err_connection", "timeout", "connect")):
                    try:
                        self.quit()
                    except Exception:
                        pass
                    raise ProxyError(self.__proxy, str(e)) from e
            logging.warning("Не удалось принять куки: %s", e)

    def _validate_proxy_page(self) -> None:
        """Проверить, что страница загрузилась (не прокси-ошибка)."""
        src = (self.page_source or "").lower()
        if "marktplaats" not in src:
            try:
                self.quit()
            except Exception:
                pass
            raise ProxyError(self.__proxy, "Страница не загрузилась (нет marktplaats в ответе)")

    def _track_clicks_mode(self, url: str) -> None:
        """Режим отслеживания кликов: пользователь кликает, мы логируем элемент."""
        try:
            for by, sel in CONSENT_IFRAME_SELECTORS:
                WebDriverWait(self, 10).until(EC.presence_of_element_located((by, sel)))
                break
        except TimeoutException:
            pass
        # Сохраняем HTML для анализа структуры cookie-баннера
        html_path = os.path.join(os.getcwd(), "debug_track_clicks.html")
        try:
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(self.page_source)
            logging.info("HTML сохранён в %s (пришлите этот файл для анализа)", html_path)
        except OSError as e:
            logging.warning("Не удалось сохранить HTML: %s", e)
        # Сохраняем результат в window.top чтобы ловить клики из iframe
        inject_script = """
            var win = window.top;
            win.__lastClicked = null;
            function captureClick(e) {
                var el = e.target;
                var info = {
                    tag: el.tagName,
                    id: el.id || '',
                    className: (typeof el.className === 'string' ? el.className : (el.className && el.className.baseVal || '')) || '',
                    ariaLabel: el.getAttribute('aria-label') || '',
                    title: el.getAttribute('title') || '',
                    text: (el.innerText || el.textContent || '').substring(0, 50),
                    xpath: (function getXPath(el) {
                        if (!el || !el.ownerDocument) return '';
                        var parts = [];
                        while (el && el.nodeType === 1) {
                            var idx = 1, sib = el.previousSibling;
                            while (sib) { if (sib.nodeType===1 && sib.tagName===el.tagName) idx++; sib = sib.previousSibling; }
                            parts.unshift((el.tagName || '').toLowerCase() + '[' + idx + ']');
                            el = el.parentNode;
                        }
                        return '/' + parts.join('/');
                    })(el),
                    inIframe: (window !== window.top),
                    inShadow: !!(el.getRootNode && el.getRootNode() instanceof ShadowRoot)
                };
                win.__lastClicked = info;
                console.log('Clicked:', info);
            }
            document.addEventListener('click', captureClick, true);
            // Shadow DOM: добавляем слушатель в #notice если есть shadowRoot
            var notice = document.getElementById('notice');
            if (notice && notice.shadowRoot) {
                notice.shadowRoot.addEventListener('click', captureClick, true);
            }
        """
        self.execute_script(inject_script)
        # Добавляем слушатели во все iframe (cookie-баннер часто в iframe)
        try:
            iframes = self.find_elements(By.TAG_NAME, "iframe")
            for i, iframe in enumerate(iframes):
                try:
                    self.switch_to.frame(iframe)
                    self.execute_script(inject_script)
                    self.switch_to.default_content()
                except Exception:
                    self.switch_to.default_content()
        except Exception:
            pass
        logging.info(">>> КЛИКНИТЕ на кнопку куки в браузере, затем нажмите Enter здесь <<<")
        input()
        info = self.execute_script("return window.top && window.top.__lastClicked;")
        if info:
            logging.info("=== Информация о кликнутом элементе ===")
            logging.info("  tag: %s", info.get("tag", ""))
            logging.info("  id: %s", info.get("id", ""))
            logging.info("  class: %s", info.get("className", ""))
            logging.info("  aria-label: %s", info.get("ariaLabel", ""))
            logging.info("  title: %s", info.get("title", ""))
            logging.info("  text: %s", info.get("text", ""))
            logging.info("  xpath: %s", info.get("xpath", ""))
            logging.info("  в iframe: %s, в shadow DOM: %s", info.get("inIframe", False), info.get("inShadow", False))
            if info.get("id"):
                logging.info("  CSS: #%s", info["id"])
            if info.get("ariaLabel"):
                logging.info("  CSS: [aria-label='%s']", info["ariaLabel"])
            logging.info("========================================")
        else:
            logging.warning("Клик не зафиксирован. Убедитесь, что кликнули по элементу.")

    def _do_accept_cookies(self) -> None:
        """Try to find and click cookie button. WebDriverWait для появления элементов."""
        # 1. Ждём consent iframe (SourcePoint) — WebDriverWait вместо sleep
        for by, selector in CONSENT_IFRAME_SELECTORS:
            try:
                iframe = WebDriverWait(self, 10).until(
                    EC.presence_of_element_located((by, selector))
                )
                logging.info("Найден consent iframe, переключаюсь...")
                self.switch_to.frame(iframe)
                clicked = self._click_cookie_button()
                self.switch_to.default_content()
                if clicked:
                    return
            except TimeoutException:
                continue
            except WebDriverException:
                self.switch_to.default_content()

        self.switch_to.default_content()
        clicked = self._click_cookie_button()
        if clicked:
            return

        # 2. Остальные iframe
        iframes = self.find_elements(By.TAG_NAME, "iframe")
        for iframe in iframes:
            try:
                self.switch_to.frame(iframe)
                clicked = self._click_cookie_button()
                self.switch_to.default_content()
                if clicked:
                    return
            except WebDriverException:
                self.switch_to.default_content()

        logging.warning("Кнопка куки не найдена (модальное окно могло измениться)")

    def _click_cookie_button(self) -> bool:
        """Try to find and click a cookie consent button. Returns True if clicked."""
        # JavaScript: ищем в документе и shadow DOM (работает в основном doc и в iframe)
        btn = self.execute_script("""
            function findBtn() {
                var sel = 'button[aria-label="Accepteren"], [aria-label="Accepteren"], '
                    + '.message-button.primary, .sp_choice_type_11, '
                    + '#notice button[aria-label="Accepteren"]';
                var el = document.querySelector(sel);
                if (el) return el;
                var notice = document.getElementById('notice');
                if (notice && notice.shadowRoot) {
                    el = notice.shadowRoot.querySelector('button[aria-label="Accepteren"]');
                    if (el) return el;
                }
                return document.evaluate(
                    "//button[contains(., 'Accepteren') or contains(., 'Accept')]",
                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null
                ).singleNodeValue;
            }
            return findBtn();
        """)
        if btn:
            self.execute_script("arguments[0].click();", btn)
            logging.debug("Куки приняты")
            return True

        for by, selector in COOKIE_SELECTORS:
            try:
                btn = WebDriverWait(self, 5).until(
                    EC.presence_of_element_located((by, selector))
                )
                self.execute_script("arguments[0].click();", btn)
                logging.debug("Куки приняты")
                return True
            except (TimeoutException, WebDriverException):
                continue
        return False

    def __get_forbidden_iframe(self) -> WebElement | None:
        """Return the forbidden iframe, or None if it does not exist."""
        try:
            iframe_elems = self.find_elements(by=By.TAG_NAME, value="iframe")
            for iframe in iframe_elems:
                src_url = iframe.get_attribute("src")
                if src_url == MARKTPLAATS_403_URL:
                    return iframe
        except StaleElementReferenceException:
            self.refresh()
        except WebDriverException:
            pass

        return None

    @staticmethod
    def __get_mp_err_text(soup: Soup) -> str | None:
        """Return the error text from the given Marktplaats page, or None."""
        err_msg_name = "p"
        err_msg_attrs = {"class": "mp-Alert--error"}
        err_msgs = soup.find_all("p", attrs=err_msg_attrs)
        if len(err_msgs) > 0:
            err_msg = err_msgs[0]
            err_text = ""

            if not isinstance(err_msg, Tag):
                raise ElementNotFound(tag_name=err_msg_name, attrs=err_msg_attrs)

            err_text = err_msg.get_text(strip=True)
            return err_text

        err_pages_name = "div"
        err_pages_attrs = {"class": "hz-ErrorPage-message"}
        err_pages = soup.find_all(name=err_pages_name, attrs=err_pages_attrs)
        if len(err_pages) > 0:
            err_page = err_pages[0]

            if not isinstance(err_page, Tag):
                raise ElementNotFound(tag_name=err_pages_name, attrs=err_pages_attrs)

            err_div_name = "div"
            err_div_attrs = {"class": "u-textStyleTitle3"}
            err_div = err_page.find(name=err_div_name, attrs=err_div_attrs)

            if not isinstance(err_div, Tag):
                raise ElementNotFound(tag_name=err_div_name, attrs=err_div_attrs)

            err_text = err_div.get_text(strip=True)
            return err_text

        return None

    def __is_cloudfront_403(self, html: str) -> bool:
        """Detect CloudFront 403 block page."""
        h = (html or "").lower()
        if "403" not in h:
            return False
        return (
            "request blocked" in h
            or "cloudfront" in h
            or "could not be satisfied" in h
            or "the request could not be satisfied" in h
            or ("blocked" in h and "request" in h)
        )

    def _check_all_frames_for_403(self) -> None:
        """Проверяет основной документ и все iframe на 403. Raises ForbiddenError если найден."""
        html = (self.page_source or "").lower()
        if self.__is_cloudfront_403(html):
            raise ForbiddenError(msg="CloudFront 403: Request blocked (rate limit / anti-bot)")
        try:
            for iframe in self.find_elements(by=By.TAG_NAME, value="iframe"):
                try:
                    self.switch_to.frame(iframe)
                    frame_html = (self.page_source or "").lower()
                    self.switch_to.default_content()
                    if self.__is_cloudfront_403(frame_html):
                        raise ForbiddenError(msg="CloudFront 403 в iframe (rate limit / anti-bot)")
                except ForbiddenError:
                    self.switch_to.default_content()
                    raise
                except WebDriverException:
                    try:
                        self.switch_to.default_content()
                    except Exception:
                        pass
        except ForbiddenError:
            raise
        except WebDriverException:
            pass

    def get_soup(self) -> Soup:
        """Return a BeautifulSoup object of the requested page, raising any Marktplaats specific errors found."""
        try:
            current_url = (self.current_url or "").lower()
            if "/403" in current_url or current_url.rstrip("/").endswith("/403"):
                raise ForbiddenError(msg="Редирект на страницу 403 (rate limit)")
        except WebDriverException:
            pass
        self._check_all_frames_for_403()
        src = self.page_source

        forbidden_iframe = self.__get_forbidden_iframe()
        if forbidden_iframe:
            self.switch_to.frame(forbidden_iframe)
            src = self.page_source
            soup = Soup(src, "lxml")
            self.switch_to.default_content()

            forbidden_err_text = self.__get_mp_err_text(soup)
            if forbidden_err_text:
                raise ForbiddenError(msg=forbidden_err_text)

        soup = Soup(src, "lxml")
        err_text = self.__get_mp_err_text(soup)
        if err_text:
            raise MPError(msg=err_text)

        return soup
