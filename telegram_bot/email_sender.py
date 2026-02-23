"""Отправка писем продавцам через Gmail SMTP.
Формат почт: mail:apppassword (только Gmail).
App Password: https://myaccount.google.com/apppasswords
"""
import logging
import os
import re
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

from .config import DB_PATH, ENVIRONMENT, TEST_MAIL
from .database import (
    get_active_template_id,
    get_template,
    get_next_email_for_listing,
    get_all_emails,
    mark_email_blocked,
    unblock_email,
    set_last_used_email,
    set_last_email_for_listing,
    format_template,
)

logger = logging.getLogger(__name__)

# В dev режиме всегда шлём на тестовую почту
DEV_TEST_RECIPIENT = "eclipselucky@gmail.com"


def _sanitize_seller_email_local(seller_name: str) -> str:
    """Из имени продавца сделать локальную часть email: только буквы/цифры, lowercase."""
    if not seller_name or not isinstance(seller_name, str):
        return "seller"
    s = re.sub(r"[^a-zA-Z0-9]", "", seller_name.lower())
    return s[:64] if s else "seller"


def _build_seller_email(seller_name: str) -> str:
    """Собрать email продавца: {seller_name}@gmail.com."""
    local = _sanitize_seller_email_local(seller_name or "")
    return f"{local}@gmail.com"


def _notify_admin_email_blocked(db_path: str, email: str, error: str) -> None:
    """Уведомить админа о заблокированной почте через Telegram."""
    admin_chat = os.getenv("ADMIN_CHAT_ID", "").strip()
    token = os.getenv("ADMIN_BOT_TOKEN", os.getenv("CLIENT_BOT_TOKEN", "")).strip()
    if not admin_chat or not token:
        return
    try:
        import json
        import urllib.request
        text = f"🚫 <b>Почта заблокирована</b>\n\n{email}\n\nПричина: {error}"
        payload = {
            "chat_id": admin_chat,
            "text": text,
            "parse_mode": "HTML",
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=data,
            method="POST",
        )
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=10) as _:
            pass
    except Exception as e:
        logger.warning("Не удалось уведомить админа о блоке почты: %s", e)


def send_seller_email(
    db_path: str,
    listing: object,
    sender_email: str,
    sender_password: str,
) -> tuple[bool, str | None]:
    """
    Отправить письмо продавцу.
    listing — объект с атрибутами: title, price_cents, listing_url, seller_name, city_name,
    category_verticals/category_ru, description, item_id.
    Возвращает True при успехе, False при ошибке (почта будет помечена blocked).
    """
    seller_name = getattr(listing, "seller_name", None) or ""
    recipient_real = _build_seller_email(seller_name)
    if ENVIRONMENT == "dev":
        recipient = DEV_TEST_RECIPIENT
        logger.info("Email (dev): отправка на %s (реальный получатель: %s)", recipient, recipient_real)
    else:
        recipient = recipient_real

    subject = f"Вопрос по объявлению «{getattr(listing, 'title', '')[:50]}»"
    if not sender_email or not sender_password:
        logger.warning("Email: нет отправителя или пароля")
        return False, None

    # Формируем тело из активного шаблона
    template_id = get_active_template_id(db_path)
    if not template_id:
        logger.warning("Email: активный шаблон не выбран")
        return False, None
    tpl = get_template(db_path, template_id)
    if not tpl:
        logger.warning("Email: шаблон %s не найден", template_id)
        return False, None
    _, body_template = tpl
    user_name = sender_email.split("@")[0] if "@" in sender_email else "User"
    cat = getattr(listing, "category_ru", None) or getattr(listing, "category_verticals", "")
    if isinstance(cat, (list, tuple)):
        cat = ", ".join(str(x) for x in cat) if cat else ""
    vars_dict = {
        "url": getattr(listing, "listing_url", "") or "",
        "title": getattr(listing, "title", "") or "",
        "price": f"€{(getattr(listing, 'price_cents', 0) or 0) / 100:.2f}",
        "price_cents": str(getattr(listing, "price_cents", 0) or 0),
        "seller_name": seller_name,
        "city": getattr(listing, "city_name", "") or "",
        "category": cat or "",
        "description": (getattr(listing, "description", "") or "")[:500],
        "user_name": user_name,
        "item_id": getattr(listing, "item_id", "") or "",
    }
    body = format_template(body_template, vars_dict)

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = formataddr((user_name, sender_email))
    msg["To"] = recipient

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(sender_email, sender_password)
            smtp.sendmail(sender_email, [recipient], msg.as_string())
        set_last_used_email(db_path, sender_email)
        set_last_email_for_listing(db_path, sender_email)
        logger.info("Email: отправлено на %s (с %s)", recipient, sender_email)
        return True, recipient
    except smtplib.SMTPAuthenticationError as e:
        err = str(e)
        logger.warning("Email auth failed for %s: %s", sender_email[:20], err)
        mark_email_blocked(db_path, sender_email)
        _notify_admin_email_blocked(db_path, sender_email, err)
        return False, None
    except Exception as e:
        err = str(e)
        logger.warning("Email send failed for %s: %s", sender_email[:20], err)
        mark_email_blocked(db_path, sender_email)
        _notify_admin_email_blocked(db_path, sender_email, err)
        return False, None


def send_test_email(
    db_path: str,
    sender_email: str,
    sender_password: str,
    recipient: str | None = None,
    mark_blocked_on_fail: bool = True,
) -> bool:
    """
    Отправить тестовое письмо на recipient (по умолчанию TEST_MAIL).
    Возвращает True при успехе. При ошибке — помечает blocked (если mark_blocked_on_fail).
    """
    to_addr = (recipient or TEST_MAIL).strip()
    if not to_addr or "@" not in to_addr:
        to_addr = "eclipselucky@gmail.com"
    subject = "Тест почты — Marktplaats Scraper"
    body = "Это тестовое письмо. Почта работает."
    user_name = sender_email.split("@")[0] if "@" in sender_email else "User"
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = formataddr((user_name, sender_email))
    msg["To"] = to_addr
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(sender_email, sender_password)
            smtp.sendmail(sender_email, [to_addr], msg.as_string())
        set_last_used_email(db_path, sender_email)
        unblock_email(db_path, sender_email)  # при успехе снять блок если был
        return True
    except smtplib.SMTPAuthenticationError as e:
        err = str(e)
        logger.warning("Test email auth failed for %s: %s", sender_email[:20], err)
        if mark_blocked_on_fail:
            mark_email_blocked(db_path, sender_email)
            _notify_admin_email_blocked(db_path, sender_email, err)
        return False
    except Exception as e:
        err = str(e)
        logger.warning("Test email send failed for %s: %s", sender_email[:20], err)
        if mark_blocked_on_fail:
            mark_email_blocked(db_path, sender_email)
            _notify_admin_email_blocked(db_path, sender_email, err)
        return False


def test_all_emails(db_path: str) -> tuple[int, int, list[str]]:
    """
    Протестировать все почты. Шлёт тест на TEST_MAIL.
    Возвращает (ok_count, failed_count, failed_emails).
    """
    all_emails = get_all_emails(db_path)
    ok_count = 0
    failed_count = 0
    failed_emails: list[str] = []
    for email, password, blocked in all_emails:
        if send_test_email(db_path, email, password):
            ok_count += 1
        else:
            failed_count += 1
            failed_emails.append(email)
    return ok_count, failed_count, failed_emails


def try_send_listing_email(db_path: str, listing: object) -> tuple[bool, str | None]:
    """
    Round-robin по активным почтам: 1-е объявление — почта 1, 2-е — почта 2, 3-е — почта 3,
    далее цикл. Только не заблокированные почты.
    При ошибке — пометить почту blocked, уведомить админа.
    Возвращает (успех, recipient или None).
    """
    creds = get_next_email_for_listing(db_path)
    if not creds:
        logger.warning("Email: нет доступных почт")
        return False, None
    sender_email, sender_password = creds
    return send_seller_email(db_path, listing, sender_email, sender_password)
