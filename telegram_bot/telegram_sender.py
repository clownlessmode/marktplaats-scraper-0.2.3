"""Отправка объявлений в Telegram при парсинге (вне бота)."""
import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)

# Токен клиентского бота для отправки воркерам
def _get_client_token() -> str:
    return os.getenv("CLIENT_BOT_TOKEN", os.getenv("BOT_TOKEN", "")).strip()

# HTML: экранировать только & < >
def _escape_html(text: str) -> str:
    if not text:
        return ""
    s = str(text)
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _get_chat_id() -> str:
    from .config import TELEGRAM_CHAT_ID
    return TELEGRAM_CHAT_ID or ""


def _format_listing_html(listing) -> str:
    """Сообщение в HTML с кликабельной ссылкой (надёжнее MarkdownV2)."""
    title = (getattr(listing, "title", None) or "")[:200]
    price = getattr(listing, "price_cents", None)
    price_str = f"€{price / 100:.2f}" if price is not None and price > 0 else "Цена по запросу"
    listing_url = getattr(listing, "listing_url", None) or ""
    city = getattr(listing, "city_name", None) or ""
    cat_ru = getattr(listing, "category_ru", None) or ""
    if not cat_ru:
        cat = getattr(listing, "category_verticals", None)
        category = ", ".join(cat) if isinstance(cat, (list, tuple)) else (cat or "")
    else:
        category = cat_ru
    seller = getattr(listing, "seller_name", None) or ""
    views = getattr(listing, "view_count", None) or 0
    favs = getattr(listing, "favorited_count", None) or 0

    parts = [f"🆕 <b>{_escape_html(title)}</b>", "", f"💰 {_escape_html(price_str)}"]
    if city:
        parts.append(f"📍 {_escape_html(city)}")
    if category:
        parts.append(f"📂 {_escape_html(category)}")
    extras = []
    if seller:
        extras.append(f"👤 {_escape_html(seller)}")
    if views:
        extras.append(f"👁 {views}")
    if favs:
        extras.append(f"⭐ {favs}")
    if extras:
        parts.append(" ".join(extras))
    parts.append("")
    if listing_url:
        url_esc = _escape_html(listing_url)
        parts.append(f'<a href="{url_esc}">🔗 Открыть объявление</a>')

    return "\n".join(parts)


def _format_listing_plain(listing) -> str:
    """Простой текст без Markdown (fallback при ошибке парсинга)."""
    title = (getattr(listing, "title", None) or "")[:200]
    price = getattr(listing, "price_cents", None)
    price_str = f"€{price / 100:.2f}" if price is not None and price > 0 else "Цена по запросу"
    listing_url = getattr(listing, "listing_url", None) or ""
    city = getattr(listing, "city_name", None) or ""
    cat_ru = getattr(listing, "category_ru", None) or ""
    cat = getattr(listing, "category_verticals", None)
    category = ", ".join(cat) if isinstance(cat, (list, tuple)) else (cat_ru or cat or "")
    parts = [f"🆕 {title}", "", f"💰 {price_str}"]
    if city:
        parts.append(f"📍 {city}")
    if category:
        parts.append(f"📂 {category}")
    if listing_url:
        parts.append(listing_url)
    return "\n".join(parts)


def _get_first_image(listing) -> str | None:
    """Первая картинка из image_urls (строка через | или tuple/list)."""
    urls = getattr(listing, "image_urls", None)
    if not urls:
        return None
    if isinstance(urls, (list, tuple)):
        first = urls[0] if urls else ""
    else:
        first = str(urls).split("|")[0].strip()
    return first if first and str(first).startswith("http") else None


def send_listing_to_telegram(chat_id: str, listing) -> bool:
    """Отправить объявление в Telegram (с фото, HTML). При ошибке — fallback на plain text."""
    token = _get_client_token() or os.getenv("BOT_TOKEN", "").strip()
    if not token:
        print("      ⚠ Telegram: BOT_TOKEN не задан в .env")
        return False
    if not chat_id:
        print("      ⚠ Telegram: MP_TELEGRAM_CHAT_ID не задан")
        return False

    def _send_text(text: str, parse_mode: str | None = "HTML") -> bool:
        payload = {"chat_id": chat_id, "text": text}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data, method="POST"
        )
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status == 200

    def _send_photo(caption: str, parse_mode: str | None = "HTML") -> bool:
        first_image = _get_first_image(listing)
        if first_image:
            payload = {"chat_id": chat_id, "photo": first_image, "caption": caption}
            if parse_mode:
                payload["parse_mode"] = parse_mode
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{token}/sendPhoto",
                data=data, method="POST"
            )
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=15) as resp:
                return resp.status == 200
        return _send_text(caption, parse_mode)

    try:
        caption = _format_listing_html(listing)
        if len(caption) > 1024:
            caption = caption[:1021] + "..."
        try:
            if _send_photo(caption):
                return True
        except Exception:
            pass
        # fallback: только текст (если фото не отправилось, напр. 400)
        if _send_text(caption):
            return True
        return False
    except Exception as e:
        logger.debug("Telegram send (HTML): %s", e)
        try:
            caption = _format_listing_plain(listing)
            if len(caption) > 1024:
                caption = caption[:1021] + "..."
            try:
                if _send_photo(caption, parse_mode=None):
                    return True
            except Exception:
                pass
            if _send_text(caption, parse_mode=None):
                return True
        except Exception as e2:
            logger.debug("Telegram send (plain): %s", e2)
        print(f"      ⚠ Telegram не отправлен: {e}")
        return False


def send_text_message(chat_id: str, text: str) -> bool:
    """Отправить текстовое сообщение в Telegram. Возвращает True при успехе."""
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token or not chat_id:
        return False
    try:
        payload = {"chat_id": chat_id, "text": text}
        api_url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(api_url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status == 200
    except Exception as e:
        logger.debug("Telegram send_text: %s", e)
        return False


def send_round_summary(round_num: int, saved: int, total_sec: float, avg_sec: float, db_count: int) -> bool:
    """Отправить сводку раунда (для ENVIRONMENT=dev)."""
    chat_id = _get_chat_id()
    if not chat_id:
        return False
    text = (
        f"🔄 Раунд {round_num} завершён\n\n"
        f"📊 Сохранено: {saved} новых\n"
        f"⏱ Время: {total_sec:.1f} с\n"
        f"📈 Среднее: {avg_sec:.1f} с/категория\n\n"
        f"📦 В БД: {db_count} объявлений"
    )
    return send_text_message(chat_id, text)


def send_listings_batch(listings: list) -> None:
    """Отправить пачку объявлений в Telegram (по одному сообщению)."""
    chat_id = _get_chat_id()
    if not chat_id or not listings:
        return
    for listing in listings:
        try:
            send_listing_to_telegram(chat_id, listing)
        except Exception:
            pass


def send_listing_to_next_worker(listing, db_path: str) -> bool:
    """
    Round-robin: отправить объявление следующему воркеру на смене.
    Если воркеров нет — не отправлять.
    """
    from .database import (
        get_next_worker_for_listing,
        set_last_worker_sent,
        record_worker_listing,
    )
    from .config import DB_PATH
    db = db_path or DB_PATH
    user_id = get_next_worker_for_listing(db)
    if user_id is None:
        return False
    chat_id = str(user_id)
    ok = send_listing_to_telegram(chat_id, listing)
    if ok:
        set_last_worker_sent(db, user_id)
        item_id = getattr(listing, "item_id", None) or getattr(listing, "id", "")
        if item_id:
            record_worker_listing(db, str(item_id), user_id)
    return ok
