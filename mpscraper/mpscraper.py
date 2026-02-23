import json
from time import sleep
import re
import logging
from typing import Callable, NamedTuple
from tqdm import tqdm
from bs4 import Tag
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from datetime import datetime, timezone

from .driver import MPDriver
from .utils import diff_hours, get_utc_iso_now, get_utc_now, format_text
from .exceptions import (
    CategoryStale,
    ElementNotFound,
    ListingsError,
    ListingsInterrupt,
    MPError,
    ForbiddenError,
    ProxyError,
    UnexpectedCategoryId,
)
from .listing import Listing, ListingDetails


MARTKPLAATS_BASE_URL = "https://marktplaats.nl"
REQUEST_OPTS = "#sortBy:SORT_INDEX|sortOrder:DECREASING"

_PROXY_ERROR_PATTERNS = (
    "ERR_PROXY",
    "ERR_TUNNEL",
    "ERR_CONNECTION_REFUSED",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_TIMED_OUT",
    "ERR_TIMED_OUT",
    "ERR_CONNECTION_CLOSED",
    "Could not connect to proxy",
    "proxy connection failed",
    "net::ERR_",
)


def _is_proxy_error(msg: str) -> bool:
    """Проверить, похожа ли ошибка на сбой прокси."""
    m = (msg or "").lower()
    return any(p.lower() in m for p in _PROXY_ERROR_PATTERNS)

CONTENT_ID = "content"
SELECT_ELEM_ID = "categoryId"
DATA_ELEM_ID = "__NEXT_DATA__"
LISTING_DATA_ELEM_ID = "__CONFIG__"
LISTING_ROOT_ID = "listing-root"
ALL_CATEGORIES_ID = 0

# if a listing ID starts with this, it seems to be a sponsored advertisement post we want to ignore
MARKTPLAATS_ADVERTISEMENT_PREFIX = "a"

TYPE_KEY = "type"
SERVICE_KEY = "service"
VERTICALS_KEY = "verticals"
LOCATION_KEY = "location"
COUNTRYCODE_KEY = "countryAbbreviation"
CITY_KEY = "cityName"
SELLER_INFO_KEY = "sellerInformation"
SELLER_ID_KEY = "sellerId"
LISTED_TIMESTAMP_KEY = "since"
LISTING_KEY = "listing"
AD_TYPE_KEY = "adType"
PRICE_INFO_KEY = "priceInfo"
PRICE_TYPE_KEY = "priceType"
PRICE_CENTS_KEY = "priceCents"
STATS_KEY = "stats"
VIEW_COUNT_KEY = "viewCount"
FAVORITED_KEY = "favoritedCount"


class Category(NamedTuple):
    """Marktplaats category."""

    id: int
    url: str


class MpScraper:
    def __init__(
        self,
        headless: bool,
        timeout_seconds: float,
        wait_seconds: float,
        base_url: str = MARTKPLAATS_BASE_URL,
        chromium_path: str | None = None,
        chromedriver_path: str | None = None,
        skip_cookies: bool = False,
        skip_count: bool = False,
        debug: bool = False,
        fast: bool = False,
        proxy: str | None = None,
        block_css: bool = False,
    ) -> None:
        self.__debug = debug
        self.__skip_count = skip_count
        self.__fast = fast
        self.__proxy = proxy
        self.__driver_params = {
            "chromedriver_path": chromedriver_path,
            "chromium_path": chromium_path,
            "base_url": base_url,
            "headless": headless,
            "skip_cookies": skip_cookies,
            "proxy": proxy,
            "block_css": block_css,
        }
        self.__driver: MPDriver = MPDriver(**self.__driver_params)

        self.__base_url = base_url
        self.__wait_seconds = wait_seconds
        self.__timeout_seconds = timeout_seconds
        # Адаптивная задержка: 0.3 сек мин, +1 при 403, сброс после N успехов
        self.__adaptive_delay = 0.3
        self.__adaptive_delay_min = 0.3
        self.__adaptive_success_count = 0
        self.__adaptive_delay_max = 3600

    def _adaptive_successes_to_reset(self) -> int:
        """Чем больше задержка, тем меньше успехов нужно для сброса."""
        return max(3, 10 - int(self.__adaptive_delay) // 10)

    def _adaptive_sleep(self) -> None:
        """Пауза перед запросом (текущая адаптивная задержка)."""
        sleep(self.__adaptive_delay)

    def _adaptive_on_success(self) -> None:
        """После успешного запроса. Сброс задержки при достаточном числе успехов."""
        self.__adaptive_success_count += 1
        threshold = self._adaptive_successes_to_reset()
        if self.__adaptive_success_count >= threshold:
            if self.__adaptive_delay > self.__adaptive_delay_min:
                logging.info(
                    "Адаптивная задержка: сброс %.1f→%.1f сек (%d успехов подряд)",
                    self.__adaptive_delay,
                    self.__adaptive_delay_min,
                    self.__adaptive_success_count,
                )
            self.__adaptive_delay = self.__adaptive_delay_min
            self.__adaptive_success_count = 0

    def _adaptive_on_403(self) -> None:
        """При 403: сброс счётчика успехов."""
        self.__adaptive_success_count = 0

    def _recreate_driver_after_403(self) -> None:
        """При 403: закрыть браузер и открыть заново — перезагрузка не сбрасывает rate limit."""
        logging.info("403 → перезапуск браузера...")
        try:
            self.__driver.quit()
        except Exception as e:
            logging.warning("Ошибка при закрытии браузера: %s", e)
        sleep(2)  # дать процессу завершиться
        self.__driver = MPDriver(**self.__driver_params)
        logging.info("Браузер перезапущен ✓")

    def close(self) -> None:
        """Gracefully close the scraper."""
        self.__driver.quit()

    @staticmethod
    def __get_url_with_options(category_url: str, page_number: int) -> str:
        """Return the formatted Marktplaats URL with options and page number."""

        if category_url[-1] != "/":
            category_url = category_url + "/"

        return f"{category_url}p/{page_number}/{REQUEST_OPTS}"

    def get_parent_categories(self) -> set[Category]:
        parent_categories: set[Category] = set()

        logging.info("Загрузка главной страницы...")
        self.__driver.get(self.__base_url)
        soup = self.__driver.get_soup()
        category_li_elem_name = "li"
        category_li_elem_attrs = {"class": "CategoriesBlock-listItem"}
        category_li_elems = soup.findAll(category_li_elem_name, attrs=category_li_elem_attrs)

        for category_li_elem in category_li_elems:
            if not isinstance(category_li_elem, Tag):
                raise ElementNotFound(tag_name=category_li_elem_name, attrs=category_li_elem_attrs)

            category_a_elem_name = "a"
            category_a_elem_attrs = {"class": "hz-Link--navigation"}
            category_a_elem = category_li_elem.find(
                name=category_a_elem_name, attrs=category_a_elem_attrs
            )
            if not isinstance(category_a_elem, Tag):
                raise ElementNotFound(tag_name=category_a_elem_name, attrs=category_a_elem_attrs)

            href_split = category_a_elem.attrs["href"].split("/")
            category_id = int(href_split[2])
            category_url = f"{self.__base_url}/l/{href_split[3]}"
            category = Category(id=category_id, url=category_url)
            parent_categories.add(category)

        if len(parent_categories) == 0:
            logging.info("HTML-парсинг не нашёл категории, пробую __CONFIG__.categoryLinks...")
            try:
                config = self.__driver.execute_script("return window.__CONFIG__")
                if config and "categoryLinks" in config:
                    for link in config["categoryLinks"]:
                        cat_id = int(link.get("id", 0))
                        url_path = link.get("url", "")
                        if url_path.startswith("/cp/"):
                            parts = url_path.rstrip("/").split("/")
                            slug = parts[-1] if len(parts) >= 3 else ""
                            if slug:
                                cat_url = f"{self.__base_url}/l/{slug}/"
                                parent_categories.add(Category(id=cat_id, url=cat_url))
                    logging.info("Из __CONFIG__ получено %d категорий", len(parent_categories))
            except Exception as exc:
                logging.warning("Не удалось получить категории из __CONFIG__: %s", exc)

        return parent_categories

    def __get_subcategories(self, parent_category: Category) -> set[Category]:
        """Return any existing sub-category URLs for the given category URL."""
        subcategories: set[Category] = set()

        logging.debug("Загрузка категории: %s", parent_category.url.split("/")[-1])
        self.__driver.get(parent_category.url)

        try:
            logging.debug("Ожидание контента (id=%s)...", parent_category.id)
            categories_present = EC.presence_of_element_located((By.ID, str(parent_category.id)))
            _ = WebDriverWait(self.__driver, self.__timeout_seconds).until(categories_present)
        except TimeoutException:
            logging.debug("Таймаут загрузки категории")
            return subcategories

        logging.debug("Парсинг подкатегорий...")
        soup = self.__driver.get_soup()

        category_id_elems_name = "select"
        category_id_elems_attrs = {"id": SELECT_ELEM_ID}
        category_id_elems = soup.find(name=category_id_elems_name, attrs=category_id_elems_attrs)
        if not isinstance(category_id_elems, Tag):
            raise ElementNotFound(tag_name=category_id_elems_name, attrs=category_id_elems_attrs)

        category_id_list_name = "div"
        category_id_list_attrs = {"id": str(parent_category.id)}
        category_id_list = soup.find(category_id_list_name, attrs=category_id_list_attrs)
        if not isinstance(category_id_list, Tag):
            raise ElementNotFound(tag_name=category_id_list_name, attrs=category_id_list_attrs)

        category_hrefs: dict[str, str] = {}
        subcategory_a_elem_name = "a"
        subcategory_a_elem_attrs = {"class": "category-name"}
        subcategory_a_elems = soup.findAll(subcategory_a_elem_name, attrs=subcategory_a_elem_attrs)
        for subcategory_a_elem in subcategory_a_elems:
            if not isinstance(subcategory_a_elem, Tag):
                raise ElementNotFound(tag_name=subcategory_a_elem)

            if "href" not in subcategory_a_elem.attrs:
                # TODO: Raise error
                continue

            category_name = str(subcategory_a_elem.contents[0])
            category_href = str(subcategory_a_elem.attrs["href"])
            category_hrefs[category_name] = category_href

        subcategory_option_elem_name = "option"
        subcategory_option_elems = category_id_elems.findAll(name=subcategory_option_elem_name)
        for subcategory_option_elem in subcategory_option_elems:
            if not isinstance(subcategory_option_elem, Tag):
                raise ElementNotFound(tag_name=subcategory_option_elem_name)

            subcategory_name = str(subcategory_option_elem.contents[0])

            if "value" not in subcategory_option_elem.attrs:
                continue

            subcategory_value = subcategory_option_elem.attrs["value"]
            if subcategory_value == "":
                continue

            subcategory_id = int(subcategory_value)

            if subcategory_id == parent_category.id or subcategory_id == ALL_CATEGORIES_ID:
                continue

            # get subcategory href
            subcategory_href = category_hrefs[subcategory_name]

            subcategory_url = f"{MARTKPLAATS_BASE_URL}{subcategory_href}"
            subcategory = Category(id=subcategory_id, url=subcategory_url)
            subcategories.add(subcategory)

        return subcategories

    def listings_count(self, category: Category) -> int:
        """Return the listings count for the given category URL."""
        logging.debug("Подсчёт объявлений: %s", category.url.split("/")[-1])
        self.__driver.get(category.url)

        page_content_present = EC.presence_of_element_located((By.ID, CONTENT_ID))
        _ = WebDriverWait(self.__driver, self.__timeout_seconds).until(page_content_present)

        soup = self.__driver.get_soup()

        label_altijd_name = "label"
        label_altijd_attrs = {"for": "offeredSince-Altijd"}
        label_altijd = soup.find(name=label_altijd_name, attrs=label_altijd_attrs)
        if isinstance(label_altijd, Tag):
            span_altijd_counter_name = "span"
            span_altijd_counter_attrs = {"class": "hz-SelectionInput-Counter"}
            span_altijd_counter = label_altijd.find(
                name=span_altijd_counter_name, attrs=span_altijd_counter_attrs
            )
            if isinstance(span_altijd_counter, Tag):
                altijd_counter_name = "span"
                altijd_counter_attrs = {"class": "hz-Text"}
                altijd_counter = span_altijd_counter.find(
                    name=altijd_counter_name, attrs=altijd_counter_attrs
                )
                if isinstance(altijd_counter, Tag):
                    count_text = altijd_counter.get_text(strip=True)
                    count_text = re.sub("[.,()]", "", count_text)
                    return int(count_text)
                else:
                    raise ElementNotFound(tag_name=altijd_counter_name, attrs=altijd_counter_attrs)
            else:
                raise ElementNotFound(
                    tag_name=span_altijd_counter_name, attrs=span_altijd_counter_attrs
                )
        else:
            raise ElementNotFound(tag_name=label_altijd_name, attrs=label_altijd_attrs)

    def __get_listing_details(self, listing_url: str) -> ListingDetails:
        """Return the full description, listing type and service attributes."""
        self.__driver.get(listing_url)

        try:
            listing_present = EC.presence_of_element_located((By.ID, LISTING_ROOT_ID))
            _ = WebDriverWait(self.__driver, self.__timeout_seconds).until(listing_present)
        except TimeoutException:
            # pass since we catch errors next
            pass

        page = self.__driver.get_soup()

        description_div_name = "div"
        description_div_attrs = {"class": "Description-description"}
        description_div = page.find(name=description_div_name, attrs=description_div_attrs)
        if not isinstance(description_div, Tag):
            raise ElementNotFound(tag_name=description_div_name, attrs=description_div_attrs)

        description = description_div.get_text(separator=" ", strip=True)

        types: set[str] = set()
        services: set[str] = set()

        # Parse type/service attributes
        attribute_items = page.find_all("div", {"class": "Attributes-item"})
        for attribute_item in attribute_items:
            if isinstance(attribute_item, Tag):
                attribute_label = attribute_item.find("strong", {"class": "Attributes-label"})
                if isinstance(attribute_label, Tag):
                    attribute_label_text = attribute_label.get_text(strip=True).lower()

                    attribute_value = attribute_item.find("span", {"class": "Attributes-value"})
                    if isinstance(attribute_value, Tag):
                        attribute_text = attribute_value.get_text(strip=True)
                        values = set()
                        if ", " in attribute_text:
                            values = set(attribute_text.split(", "))
                        else:
                            values.add(attribute_text)

                        if attribute_label_text == TYPE_KEY:
                            types = types.union(values)
                        elif attribute_label_text == SERVICE_KEY:
                            services = services.union(values)
        # Get stats
        data = self.__driver.execute_script(f"return window.{LISTING_DATA_ELEM_ID}")
        ad_type = data[LISTING_KEY][AD_TYPE_KEY]

        price_info = data[LISTING_KEY][PRICE_INFO_KEY]
        price_type = price_info[PRICE_TYPE_KEY]
        price_cents = int(price_info[PRICE_CENTS_KEY])

        stats = data[LISTING_KEY][STATS_KEY]
        view_count = int(stats[VIEW_COUNT_KEY])
        favorited_count = int(stats[FAVORITED_KEY])
        listed_timestamp = datetime.fromisoformat(stats[LISTED_TIMESTAMP_KEY]).isoformat()

        return ListingDetails(
            ad_type=ad_type,
            description=description,
            types=types,
            services=services,
            price_type=price_type,
            price_cents=price_cents,
            view_count=view_count,
            favorited_count=favorited_count,
            listed_timestamp=listed_timestamp,
        )

    def __listing_from_res_listing(
        self,
        res_listing: dict,
        parent_category: Category,
        child_category_id: int,
    ) -> Listing:
        """Build Listing from __NEXT_DATA__ searchRequestAndResponse.listings item (fast mode, no detail page)."""
        item_id = res_listing["itemId"]
        title = format_text(str(res_listing["title"]))
        vip_url = res_listing["vipUrl"]
        listing_url = f"{MARTKPLAATS_BASE_URL}{vip_url}"

        description = res_listing.get("description") or res_listing.get("categorySpecificDescription") or ""
        description = format_text(description)

        price_info = res_listing.get("priceInfo", {})
        price_type = str(price_info.get("priceType", ""))
        price_cents = int(price_info.get("priceCents", 0))

        image_urls: list[str] = []
        if "pictures" in res_listing:
            for pic in res_listing["pictures"]:
                url = pic.get("extraExtraLargeUrl") or pic.get("largeUrl") or pic.get("mediumUrl")
                if url:
                    image_urls.append(url)

        country_code = ""
        city_name = ""
        if LOCATION_KEY in res_listing:
            loc = res_listing[LOCATION_KEY]
            country_code = str(loc.get(COUNTRYCODE_KEY, ""))
            city_name = str(loc.get(CITY_KEY, ""))

        verticals: list[str] = res_listing.get(VERTICALS_KEY, [])

        seller_id = ""
        seller_name = ""
        is_verified = False
        seller_website_url = ""
        if SELLER_INFO_KEY in res_listing:
            si = res_listing[SELLER_INFO_KEY]
            seller_id = str(si.get(SELLER_ID_KEY, ""))
            seller_name = str(si.get("sellerName", ""))
            is_verified = bool(si.get("isVerified", False))
            seller_website_url = str(si.get("sellerWebsiteUrl", "") or "")

        listed_timestamp = res_listing.get("date", "")  # "Vandaag", "Gisteren", etc.
        crawled_timestamp = get_utc_iso_now()

        ad_type = price_type  # list page has no adType, use priceType as proxy

        types_list: list[str] = []
        services_list: list[str] = []
        attrs_combined: list[dict] = []
        for attr in res_listing.get("attributes", []) + res_listing.get("extendedAttributes", []):
            attrs_combined.append(attr)
            key = (attr.get("key") or "").lower()
            val = attr.get("value")
            if val and key in ("type", "soort"):
                types_list.append(str(val))
            elif val and key in ("service", "dienst", "delivery", "bezorging"):
                services_list.append(str(val))

        # Расширенные поля из __NEXT_DATA__
        latitude = 0.0
        longitude = 0.0
        distance_meters = -1
        country_name = ""
        if SELLER_INFO_KEY in res_listing:
            si = res_listing[SELLER_INFO_KEY]
            seller_name = str(si.get("sellerName", ""))
        if LOCATION_KEY in res_listing:
            loc = res_listing[LOCATION_KEY]
            latitude = float(loc.get("latitude", 0) or 0)
            longitude = float(loc.get("longitude", 0) or 0)
            distance_meters = int(loc.get("distanceMeters", -1) or -1)
            country_name = str(loc.get("countryName", ""))

        priority_product = str(res_listing.get("priorityProduct", ""))
        traits = tuple(res_listing.get("traits", []) or [])
        category_specific_description = format_text(
            res_listing.get("categorySpecificDescription") or ""
        )
        reserved = bool(res_listing.get("reserved", False))
        nap_available = bool(res_listing.get("napAvailable", False))
        urgency_feature_active = bool(res_listing.get("urgencyFeatureActive", False))

        attributes_json = json.dumps(attrs_combined, ensure_ascii=False) if attrs_combined else ""

        return Listing(
            item_id=item_id,
            parent_category_id=parent_category.id,
            child_category_id=child_category_id,
            category_verticals=tuple(verticals),
            ad_type=ad_type,
            title=title,
            description=description,
            types=tuple(types_list),
            services=tuple(services_list),
            price_type=price_type,
            price_cents=price_cents,
            image_urls=tuple(image_urls),
            listing_url=listing_url,
            country_code=country_code,
            city_name=city_name,
            seller_id=seller_id,
            listed_timestamp=listed_timestamp,
            crawled_timestamp=crawled_timestamp,
            view_count=0,
            favorited_count=0,
            seller_name=seller_name,
            latitude=latitude,
            longitude=longitude,
            distance_meters=distance_meters,
            country_name=country_name,
            priority_product=priority_product,
            traits=traits,
            category_specific_description=category_specific_description,
            reserved=reserved,
            nap_available=nap_available,
            urgency_feature_active=urgency_feature_active,
            is_verified=is_verified,
            seller_website_url=seller_website_url,
            attributes_json=attributes_json,
        )

    def get_listings(
        self,
        parent_category: Category,
        limit: int,
        existing_item_ids: set[str] | None = None,
        on_batch: Callable[[list[Listing]], None] | None = None,
        max_age_hours: float | None = None,
        on_new_listing: Callable[[Listing], None] | None = None,
    ) -> list[Listing]:
        """Return a list of Marktplaats listings for the given category, up to limit in quantity, and excluding item_ids from existing_item_ids.
        If max_age_hours is set and fast=False: stop category when a listing is older than max_age_hours (raise CategoryStale).
        If on_new_listing is set: call it for each new listing before appending."""
        listings: list[Listing] = []
        item_ids: set[str] = existing_item_ids.copy() if existing_item_ids is not None else set()
        parent_category_slug = parent_category.url.rstrip("/").split("/")[-1]

        if self.__skip_count and limit > 0:
            listings_count = limit
        else:
            try:
                listings_count = self.listings_count(parent_category)
            except Exception as exc:
                logging.debug("Подсчёт объявлений: %s", exc)
                listings_count = limit if limit > 0 else 100

        if limit > listings_count or limit == 0:
            limit = listings_count
        max_listings = max(limit, 1)  # Always try at least 1 page
        if max_age_hours is not None:
            logging.info("Цель: %d объявлений (только <%.0f ч)", max_listings, max_age_hours)
        else:
            logging.info("Цель: %d объявлений", max_listings)

        categories: list[Category] = []

        try:
            subcategories = self.__get_subcategories(parent_category=parent_category)
            if len(subcategories) > 0:
                categories = list(subcategories)
                logging.info("Подкатегории: %d шт.", len(categories))
            else:
                categories = [parent_category]
        except Exception as exc:
            logging.debug("Подкатегории: %s", exc)
            categories = [parent_category]

        with tqdm(
            desc=f'Категория "{parent_category_slug}"',
            total=max_listings,
            position=1,
            smoothing=0,
        ) as pbar:
            for cat_idx, (category_id, category_url) in enumerate(categories):
                current_page = 1
                cat_slug = category_url.rstrip("/").split("/")[-1]
                logging.info("Подкатегория %d/%d: %s", cat_idx + 1, len(categories), cat_slug)
                while len(listings) < max_listings:
                    try:
                        url_with_opts = self.__get_url_with_options(category_url, current_page)
                        logging.info("Страница %d: %s", current_page, cat_slug)
                        self._adaptive_sleep()
                        self.__driver.get(url_with_opts)

                        # Wait for __NEXT_DATA__ (page may load slowly or need hydration)
                        try:
                            next_data_present = EC.presence_of_element_located((By.ID, DATA_ELEM_ID))
                            WebDriverWait(self.__driver, max(15, self.__timeout_seconds)).until(next_data_present)
                        except TimeoutException:
                            pass  # continue, will fail below with clearer error

                        # Attempt to parse the page
                        page = self.__driver.get_soup()

                        # Get the next.js props JSON object (id="__NEXT_DATA__")
                        page_data_script = page.find("script", attrs={"id": DATA_ELEM_ID})
                        next_data_json: str | None = None
                        if isinstance(page_data_script, Tag) and page_data_script.text:
                            next_data_json = page_data_script.text
                        if not next_data_json:
                            next_data_json = self.__driver.execute_script(
                                "var el = document.getElementById('__NEXT_DATA__'); return el ? el.textContent : null;"
                            )
                        if not next_data_json:
                            # 403 / rate limit — проверяем основной документ и все iframe
                            self.__driver._check_all_frames_for_403()
                            page_src = (self.__driver.page_source or "").lower()
                            if any(
                                p in page_src
                                for p in ("rate limit", "too many requests", "blocked", "captcha")
                            ):
                                raise ForbiddenError(msg="Страница заблокирована (rate limit / anti-bot)")
                            script_ids = [s.get("id", "no-id") for s in page.find_all("script")[:10]]
                            # Пустая страница или нет скриптов — вероятно 403/rate limit
                            if not script_ids or len(page.find_all("script")) < 2:
                                logging.debug("Страница пустая/блок: %s", script_ids)
                                raise ForbiddenError(
                                    msg="Страница пустая или заблокирована (rate limit / 403)"
                                )
                            logging.debug("Скрипт не найден: %s", script_ids)
                            if self.__debug:
                                import os
                                debug_path = os.path.join(os.getcwd(), f"debug_{cat_slug}_p{current_page}.html")
                                with open(debug_path, "w", encoding="utf-8") as f:
                                    f.write(page.prettify())
                                logging.info("Страница сохранена в %s", debug_path)
                            raise ElementNotFound(
                                tag_name="script",
                                attrs={"id": DATA_ELEM_ID},
                            )

                        page_data = json.loads(next_data_json)
                        page_props = page_data.get("props", {}).get("pageProps", {})

                        # 403 — ждём и повторяем; 404 и др. — пропускаем категорию
                        if "errorStatusCode" in page_props:
                            err_code = page_props["errorStatusCode"]
                            if err_code == 403:
                                raise ForbiddenError(
                                    msg=f"Категория {cat_slug} вернула 403 (rate limit)"
                                )
                            logging.info("  → категория недоступна (%s)", err_code)
                            break

                        self._adaptive_on_success()

                        try:
                            res_listings = page_props["searchRequestAndResponse"]["listings"]
                        except KeyError as e:
                            pp_keys = list(page_data.get("props", {}).get("pageProps", {}).keys())[:15]
                            logging.warning("Структура страницы изменилась: %s", str(e)[:60])
                            if self.__debug:
                                import json as _json
                                import os
                                debug_path = os.path.join(os.getcwd(), f"debug_{cat_slug}_nextdata.json")
                                with open(debug_path, "w", encoding="utf-8") as f:
                                    _json.dump(page_data, f, indent=2, ensure_ascii=False)
                                logging.info("__NEXT_DATA__ сохранён в %s", debug_path)
                            raise

                        if len(res_listings) == 0:
                            logging.info("  → конец списка")
                            break

                        logging.info("  → %d объявлений", len(res_listings))

                        page_listings: list[Listing] = []
                        for res_listing in res_listings:
                            if len(listings) == limit:
                                break

                            item_id: str = res_listing["itemId"]
                            if item_id[0] == MARKTPLAATS_ADVERTISEMENT_PREFIX:
                                # skip sponsored advertisement listings
                                continue

                            # skip if we already have this item_id
                            if item_id in item_ids:
                                # if limit is set to max listings then decrease the maximum fetch-able listings count
                                if limit == listings_count:
                                    max_listings -= 1
                                    pbar.total = max_listings

                                continue

                            # Get category ID
                            child_category_id: int = int(res_listing["categoryId"])
                            # When using parent category (no subcategories), page has mixed child IDs
                            if len(categories) > 1 and child_category_id != category_id:
                                raise UnexpectedCategoryId(child_category_id, category_id)

                            # Get basic info
                            title: str = format_text(str(res_listing["title"]))
                            vip_url = res_listing["vipUrl"]
                            listing_url = f"{MARTKPLAATS_BASE_URL}{vip_url}"

                            if self.__fast:
                                listing = self.__listing_from_res_listing(
                                    res_listing=res_listing,
                                    parent_category=parent_category,
                                    child_category_id=child_category_id,
                                )
                            else:
                                # Get image URLs
                                image_urls: list[str] = []
                                if "pictures" in res_listing:
                                    for image_path in res_listing["pictures"]:
                                        image_url_el = image_path["extraExtraLargeUrl"]
                                        image_urls.append(image_url_el)

                                # Get listing details from listing page
                                listing_details: ListingDetails | None = None
                                try:
                                    got_details = False
                                    while not got_details:
                                        try:
                                            self._adaptive_sleep()
                                            listing_details = self.__get_listing_details(listing_url)
                                            self._adaptive_on_success()
                                            got_details = True
                                        except ForbiddenError as fe:
                                            self._adaptive_on_403()
                                            if self.__proxy:
                                                raise ProxyError(self.__proxy, fe.msg or "403") from fe
                                            self._recreate_driver_after_403()
                                            continue
                                except MPError as exc:
                                    logging.warning("Пропуск «%s»: %s", title[:50], str(exc)[:80])

                                except ElementNotFound:
                                    logging.debug("Пропуск «%s»: страница изменилась", title[:50])

                                except KeyboardInterrupt as exc:
                                    raise exc

                                if listing_details is None:
                                    # Failed to get listing details, so skip this listing
                                    max_listings -= 1
                                    pbar.total = max_listings
                                    continue

                                # Watch mode: если объявление старше max_age_hours — переходим к следующей категории
                                if max_age_hours is not None:
                                    try:
                                        ts = listing_details.listed_timestamp.replace("Z", "+00:00")
                                        listed_dt = datetime.fromisoformat(ts)
                                        if listed_dt.tzinfo is None:
                                            listed_dt = listed_dt.replace(tzinfo=timezone.utc)
                                        age_hours = diff_hours(listed_dt, get_utc_now())
                                        if age_hours > max_age_hours:
                                            logging.info(
                                                "  → «%s» — %.1f ч назад (>3ч) → следующая категория",
                                                title[:50],
                                                age_hours,
                                            )
                                            raise CategoryStale(listings=listings, msg=f"Объявление старше {max_age_hours}ч")
                                        logging.info(
                                            "  ✓ «%s» — %.1f ч назад (сохраняю)",
                                            title[:50],
                                            age_hours,
                                        )
                                    except (ValueError, TypeError):
                                        pass  # не удалось распарсить — считаем новым

                                # Get location info
                                country_code = ""
                                city_name = ""
                                if LOCATION_KEY in res_listing:
                                    loc = res_listing[LOCATION_KEY]
                                    country_code = loc.get(COUNTRYCODE_KEY, "")
                                    city_name = loc.get(CITY_KEY, "")

                                # Get category name hierarchy ("verticals")
                                verticals = []
                                if VERTICALS_KEY in res_listing:
                                    verticals = res_listing[VERTICALS_KEY]

                                # get seller ID
                                seller_id = ""
                                if SELLER_INFO_KEY in res_listing:
                                    seller_id = res_listing[SELLER_INFO_KEY][SELLER_ID_KEY]

                                crawled_timestamp = get_utc_iso_now()

                                listing = Listing(
                                    item_id=item_id,
                                    parent_category_id=parent_category.id,
                                    child_category_id=child_category_id,
                                    category_verticals=tuple(verticals),
                                    ad_type=listing_details.ad_type,
                                    title=title,
                                    description=format_text(listing_details.description),
                                    types=tuple(listing_details.types),
                                    services=tuple(listing_details.services),
                                    price_type=listing_details.price_type,
                                    price_cents=listing_details.price_cents,
                                    image_urls=tuple(image_urls),
                                    listing_url=listing_url,
                                    country_code=country_code,
                                    city_name=city_name,
                                    seller_id=seller_id,
                                    listed_timestamp=listing_details.listed_timestamp,
                                    crawled_timestamp=crawled_timestamp,
                                    view_count=listing_details.view_count,
                                    favorited_count=listing_details.favorited_count,
                                )

                            if on_new_listing:
                                on_new_listing(listing)
                            listings.append(listing)
                            page_listings.append(listing)
                            item_ids.add(listing.item_id)
                            pbar.update()

                        if on_batch and page_listings:
                            on_batch(page_listings)

                        current_page += 1

                    except ForbiddenError as fe:
                        self._adaptive_on_403()
                        if self.__proxy:
                            raise ProxyError(self.__proxy, fe.msg or "403") from fe
                        self._recreate_driver_after_403()
                        continue
                    except MPError as exc:
                        logging.warning("Категория %s: %s", cat_slug, str(exc)[:60])
                        continue
                    except ElementNotFound:
                        logging.debug("Страница категории изменилась: %s", cat_slug)
                        continue
                    except TimeoutException as exc:
                        if self.__proxy and _is_proxy_error(str(exc)):
                            raise ProxyError(self.__proxy, str(exc)) from exc
                        logging.warning("%s", str(exc))
                        continue
                    except WebDriverException as exc:
                        msg = str(exc) + (exc.msg or "")
                        if self.__proxy and _is_proxy_error(msg):
                            raise ProxyError(self.__proxy, msg) from exc
                        logging.error("%s", str(exc))
                        if exc.msg and "ERR_INTERNET_DISCONNECTED" in exc.msg:
                            sleep(self.__timeout_seconds)
                            continue
                        else:
                            raise exc
                    except KeyboardInterrupt as exc:
                        raise ListingsInterrupt(listings=listings) from exc
                    except Exception as exc:
                        logging.exception(exc)
                        raise ListingsError(
                            listings=listings, msg=f"Error getting listings: {exc}"
                        ) from exc

        return listings
