"""Отправка писем продавцам через Gmail SMTP.
Формат почт: mail:apppassword (только Gmail).
App Password: https://myaccount.google.com/apppasswords
Поддержка SOCKS5 прокси (proxies.json) для обхода блокировки SMTP на VDS.
"""
import json
import logging
import os
import re
import smtplib
import traceback
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path

from .config import DB_PATH, ENVIRONMENT, TEST_MAIL
from .database import (
    get_active_template_id,
    get_template,
    get_next_email_for_listing,
    get_all_emails,
    get_emails_count,
    mark_email_blocked,
    unblock_email,
    set_last_used_email,
    set_last_email_for_listing,
    format_template,
)

logger = logging.getLogger(__name__)

# В dev режиме всегда шлём на тестовую почту
DEV_TEST_RECIPIENT = "eclipselucky@gmail.com"


def _load_smtp_proxy() -> tuple[str, int, str, str] | None:
    """Первый прокси из proxies.json для SMTP. (host, port, user, pass) или None."""
    for base in (Path.cwd(), Path(__file__).resolve().parent.parent):
        path = base / "proxies.json"
        if path.is_file():
            try:
                data = json.loads(path.read_text(encoding="utf-8").strip())
                if isinstance(data, list) and data:
                    s = str(data[0]).strip()
                    parts = s.split(":", 3)
                    if len(parts) >= 2:
                        host, port = parts[0], int(parts[1])
                        user, pass_ = (parts[2], parts[3]) if len(parts) == 4 else ("", "")
                        return (host, port, user, pass_)
            except (json.JSONDecodeError, OSError, ValueError):
                pass
            break
    return None


def _create_smtp_connection(host: str = "smtp.gmail.com", port: int = 587):
    """Создать SMTP соединение, через SOCKS5 если есть proxies.json."""
    proxy = _load_smtp_proxy()
    if proxy:
        try:
            import socks

            class SocksSMTP(smtplib.SMTP):
                def _get_socket(self, host, port, timeout=None, source_address=None):
                    sock = socks.socksocket()
                    sock.set_proxy(
                        socks.SOCKS5,
                        proxy[0],
                        proxy[1],
                        username=proxy[2] or None,
                        password=proxy[3] or None,
                    )
                    try:
                        tout = float(timeout) if timeout is not None else 30.0
                        sock.settimeout(tout)
                    except (TypeError, ValueError):
                        sock.settimeout(30.0)
                    sock.connect((host, port))
                    return sock

            smtp = SocksSMTP(host, port)
            logger.info("SMTP: соединение через SOCKS5 прокси %s:%s", proxy[0], proxy[1])
            return smtp
        except ImportError:
            logger.warning("SMTP: PySocks не установлен. pip install PySocks для отправки через прокси.")
    return smtplib.SMTP(host, port)


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
    user_id: int,
) -> tuple[bool, str | None]:
    """
    Отправить письмо продавцу. user_id — воркер, чьи почта и шаблон используются.
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
        logger.warning("SMTP: нет отправителя или пароля, пропуск")
        return False, None

    logger.info(
        "SMTP: отправка | from=%s | to=%s | subject=%s | title=%s",
        sender_email[:20] + "..." if len(sender_email) > 20 else sender_email,
        recipient,
        subject[:50],
        (getattr(listing, "title", "") or "")[:40],
    )

    # Формируем тело из активного шаблона воркера
    template_id = get_active_template_id(db_path, user_id)
    if not template_id:
        logger.warning("Email: активный шаблон не выбран у воркера %s", user_id)
        return False, None
    tpl = get_template(db_path, template_id, user_id)
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
        smtp = _create_smtp_connection()
        try:
            smtp.starttls()
            smtp.login(sender_email, sender_password)
            smtp.sendmail(sender_email, [recipient], msg.as_string())
        finally:
            smtp.quit()
        set_last_used_email(db_path, sender_email, user_id)
        set_last_email_for_listing(db_path, user_id, sender_email)
        logger.info("SMTP: OK | to=%s | from=%s", recipient, sender_email[:25])
        return True, recipient
    except smtplib.SMTPAuthenticationError as e:
        err = str(e)
        logger.error(
            "SMTP: ошибка авторизации | from=%s | to=%s | err=%s",
            sender_email[:25], recipient, err,
        )
        mark_email_blocked(db_path, sender_email, user_id)
        _notify_admin_email_blocked(db_path, sender_email, err)
        return False, None
    except Exception as e:
        err = str(e)
        err_type = type(e).__name__
        logger.error(
            "SMTP: ошибка отправки | from=%s | to=%s | err=%s | type=%s\n%s",
            sender_email[:25], recipient, err, err_type, traceback.format_exc(),
        )
        # Блокируем только при ошибке авторизации. Прокси/сеть — не блокируем (временные сбои).
        if "socks" in err_type.lower() or "proxy" in err_type.lower() or "connection" in err_type.lower() or "unreachable" in err.lower():
            logger.warning("SMTP: сетевая/прокси ошибка — почта НЕ заблокирована, можно повторить позже")
        else:
            mark_email_blocked(db_path, sender_email, user_id)
            _notify_admin_email_blocked(db_path, sender_email, err)
        return False, None


def send_test_email(
    db_path: str,
    sender_email: str,
    sender_password: str,
    recipient: str | None = None,
    mark_blocked_on_fail: bool = True,
    user_id: int = 0,
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
    logger.info("SMTP test: from=%s | to=%s", sender_email[:25], to_addr)
    try:
        smtp = _create_smtp_connection()
        try:
            smtp.starttls()
            smtp.login(sender_email, sender_password)
            smtp.sendmail(sender_email, [to_addr], msg.as_string())
        finally:
            smtp.quit()
        set_last_used_email(db_path, sender_email, user_id)
        unblock_email(db_path, sender_email, user_id)  # при успехе снять блок если был
        logger.info("SMTP test: OK | from=%s", sender_email[:25])
        return True
    except smtplib.SMTPAuthenticationError as e:
        err = str(e)
        logger.error("SMTP test: auth failed | from=%s | err=%s", sender_email[:25], err)
        if mark_blocked_on_fail:
            mark_email_blocked(db_path, sender_email, user_id)
            _notify_admin_email_blocked(db_path, sender_email, err)
        return False
    except Exception as e:
        err = str(e)
        err_type = type(e).__name__
        logger.error(
            "SMTP test: send failed | from=%s | to=%s | err=%s | type=%s\n%s",
            sender_email[:25], to_addr, err, err_type, traceback.format_exc(),
        )
        # Прокси/сеть — не блокируем
        if mark_blocked_on_fail and "socks" not in err_type.lower() and "proxy" not in err_type.lower() and "connection" not in err_type.lower() and "unreachable" not in err.lower():
            mark_email_blocked(db_path, sender_email, user_id)
            _notify_admin_email_blocked(db_path, sender_email, err)
        return False


def test_all_emails(db_path: str, user_id: int | None = None) -> tuple[int, int, list[str]]:
    """
    Протестировать все почты. user_id=None — все почты всех воркеров.
    Возвращает (ok_count, failed_count, failed_emails).
    """
    all_emails = get_all_emails(db_path, user_id)
    ok_count = 0
    failed_count = 0
    failed_emails: list[str] = []
    if user_id is not None:
        for email, password, _ in all_emails:
            if send_test_email(db_path, email, password, user_id=user_id):
                ok_count += 1
            else:
                failed_count += 1
                failed_emails.append(email)
    else:
        for uid, email, password, _ in all_emails:
            if send_test_email(db_path, email, password, user_id=uid):
                ok_count += 1
            else:
                failed_count += 1
                failed_emails.append(email)
    return ok_count, failed_count, failed_emails


def send_bulk_listing_emails(
    db_path: str,
    user_id: int,
    listings: list[dict | object],
) -> tuple[int, int, list[str]]:
    """
    Рассылка по списку товаров. Использует почты и шаблон воркера (round-robin).
    Возвращает (успешно, ошибок, список recipient при успехе).
    """
    ok_count = 0
    fail_count = 0
    recipients: list[str] = []
    db_abs = str(Path(db_path).resolve())
    total_emails = get_emails_count(db_path, user_id, include_blocked=True)
    active_emails = get_emails_count(db_path, user_id, include_blocked=False)
    template_id = get_active_template_id(db_path, user_id)
    logger.info(
        "SMTP bulk: старт | user_id=%s | db=%s | строк=%s | почт всего=%s | активных=%s | шаблон=%s",
        user_id, db_abs, len(listings), total_emails, active_emails, "есть" if template_id else "НЕТ",
    )
    if active_emails == 0:
        logger.warning("SMTP bulk: нет активных почт у user_id=%s, пропуск всей рассылки", user_id)
        return 0, len(listings), []
    if not template_id:
        logger.warning("SMTP bulk: нет активного шаблона у user_id=%s, пропуск всей рассылки", user_id)
        return 0, len(listings), []
    for i, item in enumerate(listings):
        if isinstance(item, dict):
            from types import SimpleNamespace
            ns = SimpleNamespace(**item)
        else:
            ns = item
        creds = get_next_email_for_listing(db_path, user_id)
        if not creds:
            active_now = get_emails_count(db_path, user_id, include_blocked=False)
            logger.warning(
                "SMTP bulk: нет почт у user_id=%s, строка %s/%s (активных сейчас=%s)",
                user_id, i + 1, len(listings), active_now,
            )
            fail_count += 1
            continue
        sender_email, sender_password = creds
        ok, recipient = send_seller_email(db_path, ns, sender_email, sender_password, user_id)
        if ok and recipient:
            ok_count += 1
            recipients.append(recipient)
        else:
            fail_count += 1
    logger.info(
        "SMTP bulk: завершено | user_id=%s | ok=%s | fail=%s | всего=%s",
        user_id, ok_count, fail_count, len(listings),
    )
    return ok_count, fail_count, recipients


def try_send_listing_email(db_path: str, listing: object, worker_id: int | None) -> tuple[bool, str | None]:
    """
    Round-robin по активным почтам воркера. worker_id=None — пропуск (нет воркера на смене).
    При ошибке — пометить почту blocked, уведомить админа.
    Возвращает (успех, recipient или None).
    """
    if worker_id is None:
        return False, None
    creds = get_next_email_for_listing(db_path, worker_id)
    if not creds:
        logger.warning("SMTP: нет доступных почт у воркера %s, пропуск", worker_id)
        return False, None
    sender_email, sender_password = creds
    return send_seller_email(db_path, listing, sender_email, sender_password, worker_id)
