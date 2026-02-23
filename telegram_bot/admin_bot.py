#!/usr/bin/env python3
"""Админский бот — только для ADMIN_CHAT_ID. Управление воркерами и почтами."""
import asyncio
import csv
import html
import io
import logging
from pathlib import Path

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from .config import ADMIN_BOT_TOKEN, ADMIN_CHAT_ID, CLIENT_BOT_TOKEN, DB_PATH
from .database import (
    init_db,
    get_pending_users,
    get_all_workers,
    get_workers_with_stats,
    get_blocked_users,
    authorize_user,
    block_user,
    unblock_user,
    delete_user,
    add_email,
    add_emails_batch,
    get_emails,
    get_emails_count,
    delete_email,
    unblock_email,
    get_last_used_email,
    parse_emails_text,
    parse_emails_csv,
    TEMPLATE_VARS,
    add_template,
    get_templates,
    get_template,
    update_template,
    delete_template,
    get_active_template_id,
    set_active_template_id,
    format_template_example,
)
from .email_sender import send_test_email, test_all_emails

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router()


class EmailsState(StatesGroup):
    add_text = State()


class TemplateState(StatesGroup):
    name = State()
    body = State()


def _admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Ожидают подтверждения", callback_data="admin_pending")],
        [InlineKeyboardButton(text="👥 Воркеры", callback_data="admin_workers")],
        [InlineKeyboardButton(text="🚫 Заблокированные", callback_data="admin_blocked")],
        [InlineKeyboardButton(text="📧 Почты", callback_data="admin_emails")],
        [InlineKeyboardButton(text="📝 Шаблоны", callback_data="admin_templates")],
    ])


@router.message(CommandStart())
async def cmd_start(msg: Message) -> None:
    if str(msg.chat.id) != str(ADMIN_CHAT_ID):
        return
    await msg.answer(
        "👑 <b>Панель администратора</b>\n\n"
        "Выберите действие:",
        reply_markup=_admin_kb(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "admin_main")
async def cb_admin_main(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    await cb.message.edit_text(
        "👑 <b>Панель администратора</b>\n\nВыберите действие:",
        reply_markup=_admin_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "admin_pending")
async def cb_admin_pending(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    text, kb = _render_pending()
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cb.answer()


def _render_pending() -> tuple[str, InlineKeyboardMarkup]:
    pending = get_pending_users(DB_PATH)
    if not pending:
        text = "📋 <b>Ожидающие подтверждения</b>\n\nНет заявок."
        return text, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_main")]])
    lines = ["📋 <b>Ожидающие подтверждения</b>\n"]
    btns = []
    for uid, created in pending[:15]:
        lines.append(f"• ID <code>{uid}</code> — {created[:10] if created else '?'}")
        btns.append([
            InlineKeyboardButton(text=f"✅ Одобрить {uid}", callback_data=f"approve_{uid}"),
            InlineKeyboardButton(text=f"❌ Отклонить {uid}", callback_data=f"reject_{uid}"),
        ])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_main")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=btns)


@router.callback_query(F.data.startswith("approve_"))
async def cb_approve(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    try:
        uid = int(cb.data.replace("approve_", ""))
        authorize_user(DB_PATH, uid)
        if CLIENT_BOT_TOKEN:
            try:
                client_bot = Bot(token=CLIENT_BOT_TOKEN)
                async with client_bot.context():
                    await client_bot.send_message(
                        uid,
                        "✅ <b>Ваша заявка одобрена!</b>\n\n"
                        "Теперь вы можете пользоваться ботом. Нажмите /start для начала.",
                        parse_mode="HTML",
                    )
            except Exception:
                pass
        text, kb = _render_pending()
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await cb.answer("✅ Воркер одобрен", show_alert=True)
    except Exception as e:
        await cb.answer(f"Ошибка: {e}", show_alert=True)


@router.callback_query(F.data.startswith("reject_"))
async def cb_reject(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    try:
        uid = int(cb.data.replace("reject_", ""))
        block_user(DB_PATH, uid)
        text, kb = _render_pending()
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await cb.answer("❌ Воркер отклонён и заблокирован", show_alert=True)
    except Exception as e:
        await cb.answer(f"Ошибка: {e}", show_alert=True)


def _render_workers() -> tuple[str, InlineKeyboardMarkup]:
    try:
        workers = get_workers_with_stats(DB_PATH)
    except Exception:
        workers = [{"user_id": u[0], "created_at": u[1], "shift_active": u[2], "listings_today": 0, "last_listing_at": ""}
                   for u in get_all_workers(DB_PATH)]
    if not workers:
        text = "👥 <b>Воркеры</b>\n\nНет авторизованных воркеров."
        return text, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_main")]])
    lines = ["👥 <b>Воркеры</b>\n"]
    btns = []
    for w in workers[:20]:
        uid = w["user_id"]
        shift = "🟢 на смене" if w["shift_active"] else "⚪ не на смене"
        created = (w["created_at"] or "")[:10]
        today = w.get("listings_today", 0)
        last = w.get("last_listing_at", "") or "—"
        lines.append(
            f"• ID <code>{uid}</code> — {shift}\n"
            f"  📅 Рег: {created} | 📦 Сегодня: {today} | 🕐 Последний: {last}"
        )
        btns.append([
            InlineKeyboardButton(text=f"🚫 Блок", callback_data=f"block_{uid}"),
            InlineKeyboardButton(text=f"🗑 Удалить", callback_data=f"delete_{uid}"),
        ])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_main")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=btns)


@router.callback_query(F.data == "admin_workers")
async def cb_admin_workers(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    text, kb = _render_workers()
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data.startswith("block_"))
async def cb_block_worker(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    try:
        uid = int(cb.data.replace("block_", ""))
        block_user(DB_PATH, uid)
        text, kb = _render_workers()
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await cb.answer("🚫 Воркер заблокирован", show_alert=True)
    except Exception as e:
        await cb.answer(f"Ошибка: {e}", show_alert=True)


@router.callback_query(F.data.startswith("delete_"))
async def cb_delete_worker(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    try:
        uid = int(cb.data.replace("delete_", ""))
        if delete_user(DB_PATH, uid):
            text, kb = _render_workers()
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
            await cb.answer("🗑 Воркер удалён из БД", show_alert=True)
        else:
            await cb.answer("Не найден", show_alert=True)
    except Exception as e:
        await cb.answer(f"Ошибка: {e}", show_alert=True)


def _render_blocked() -> tuple[str, InlineKeyboardMarkup]:
    blocked = get_blocked_users(DB_PATH)
    if not blocked:
        text = "🚫 <b>Заблокированные</b>\n\nНет заблокированных."
        return text, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_main")]])
    lines = ["🚫 <b>Заблокированные</b>\n"]
    btns = []
    for uid, blocked_at in blocked[:20]:
        lines.append(f"• ID <code>{uid}</code> — {blocked_at[:10] if blocked_at else '?'}")
        btns.append([InlineKeyboardButton(text=f"🔓 Разблокировать {uid}", callback_data=f"unblock_{uid}")])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_main")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=btns)


@router.callback_query(F.data == "admin_blocked")
async def cb_admin_blocked(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    text, kb = _render_blocked()
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data.startswith("unblock_"))
async def cb_unblock_worker(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    try:
        uid = int(cb.data.replace("unblock_", ""))
        unblock_user(DB_PATH, uid)
        text, kb = _render_blocked()
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await cb.answer("🔓 Разблокирован (нужно заново одобрить)", show_alert=True)
    except Exception as e:
        await cb.answer(f"Ошибка: {e}", show_alert=True)


# --- Почты ---
def _emails_menu_kb(page: int = 0) -> InlineKeyboardMarkup:
    count = get_emails_count(DB_PATH)
    btns = [
        [InlineKeyboardButton(text="➕ Добавить (mail:apppassword)", callback_data="emails_add")],
        [InlineKeyboardButton(text="📤 Загрузить CSV", callback_data="emails_upload")],
        [InlineKeyboardButton(text=f"📋 Список ({count})", callback_data="emails_list_0")],
        [
            InlineKeyboardButton(text="📧 Тест почты", callback_data="emails_test"),
            InlineKeyboardButton(text="🔄 Протестировать все", callback_data="emails_test_all"),
        ],
        [InlineKeyboardButton(text="📥 Экспорт CSV", callback_data="emails_export")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=btns)


@router.callback_query(F.data == "admin_emails")
async def cb_admin_emails(cb: CallbackQuery, state: FSMContext) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    await state.clear()
    count = get_emails_count(DB_PATH)
    await cb.message.edit_text(
        f"📧 <b>База почт</b>\n\nВсего: {count}\n\n"
        "• Добавить — введите mail:apppassword (только Gmail, через Enter — несколько)\n"
        "• Загрузить CSV — пришлите файл .csv\n"
        "• Список — просмотр и удаление",
        reply_markup=_emails_menu_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "emails_add")
async def cb_emails_add(cb: CallbackQuery, state: FSMContext) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    await state.set_state(EmailsState.add_text)
    await cb.message.edit_text(
        "➕ <b>Добавить почты</b>\n\n"
        "Только Gmail. Формат:\n"
        "<code>mail@gmail.com:apppassword</code>\n\n"
        "App Password: myaccount.google.com/apppasswords\n\n"
        "Несколько строк — через Enter:\n"
        "<code>email1@x.com:pass1</code>\n"
        "<code>email2@x.com:pass2</code>\n\n"
        "Разделители: <code>:</code> <code>;</code> <code>Tab</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_emails")],
        ]),
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(EmailsState.add_text, F.text)
async def msg_emails_add_text(msg: Message, state: FSMContext) -> None:
    if str(msg.chat.id) != str(ADMIN_CHAT_ID):
        return
    pairs = parse_emails_text(msg.text or "")
    if not pairs:
        await msg.answer("❌ Не найдено валидных строк. Формат: mail@gmail.com:apppassword")
        return
    added, skipped = add_emails_batch(DB_PATH, pairs)
    await state.clear()
    await msg.answer(f"✅ Добавлено: {added}, пропущено (дубли): {skipped}")
    await msg.answer("📧 База почт", reply_markup=_emails_menu_kb())


@router.message(F.document)
async def msg_emails_csv(msg: Message, state: FSMContext) -> None:
    if str(msg.chat.id) != str(ADMIN_CHAT_ID):
        return
    doc = msg.document
    if not doc or not doc.file_name:
        return
    if not doc.file_name.lower().endswith(".csv"):
        await msg.answer("❌ Нужен файл .csv")
        return
    try:
        file = await msg.bot.get_file(doc.file_id)
        data = await msg.bot.download_file(file.file_path)
        content = data.read().decode("utf-8", errors="replace")
        pairs = parse_emails_csv(content)
        if not pairs:
            await msg.answer("❌ В CSV не найдено email. Колонки: email, apppassword (только Gmail)")
            return
        added, skipped = add_emails_batch(DB_PATH, pairs)
        await msg.answer(f"✅ Из CSV добавлено: {added}, пропущено (дубли): {skipped}")
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}")


@router.callback_query(F.data.startswith("emails_list_"))
async def cb_emails_list(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    try:
        page = int(cb.data.replace("emails_list_", ""))
    except ValueError:
        page = 0
    text, kb = _render_emails_list(page)
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cb.answer()


def _render_emails_list(page: int) -> tuple[str, InlineKeyboardMarkup]:
    per_page = 15
    offset = page * per_page
    rows = get_emails(DB_PATH, limit=per_page, offset=offset)
    total = get_emails_count(DB_PATH)
    last_used = get_last_used_email(DB_PATH)
    if not rows:
        return "📋 <b>Список почт</b>\n\nПусто.", InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К меню почт", callback_data="admin_emails")],
        ])
    lines = [f"📋 <b>Почты</b> (стр. {page + 1}, всего {total})\n"]
    btns = []
    for email, password, _, blocked in rows:
        mask = email[:3] + "***" + email[email.index("@"):] if "@" in email else email
        safe = email.replace("_", "__").replace("@", "_a_").replace(":", "_c_")
        if email == last_used and not blocked:
            badge = " ✉️ активна"
        elif blocked:
            badge = " 🚫"
        else:
            badge = ""
        lines.append(f"• <code>{email}</code>{badge}")
        row_btns = []
        if blocked:
            row_btns.append(InlineKeyboardButton(text=f"↩️ Разблокировать", callback_data=f"emails_unblock_{page}_{safe}"))
        row_btns.append(InlineKeyboardButton(text=f"🗑 {mask}", callback_data=f"emails_del_{page}_{safe}"))
        btns.append(row_btns)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"emails_list_{page - 1}"))
    if offset + len(rows) < total:
        nav.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"emails_list_{page + 1}"))
    btns.append(nav)
    btns.append([InlineKeyboardButton(text="◀️ К меню почт", callback_data="admin_emails")])
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3997] + "..."
    return text, InlineKeyboardMarkup(inline_keyboard=btns)


@router.callback_query(F.data.startswith("emails_unblock_"))
async def cb_emails_unblock(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    parts = cb.data.replace("emails_unblock_", "").split("_", 1)
    page = int(parts[0]) if parts and parts[0].isdigit() else 0
    safe = parts[1] if len(parts) > 1 else ""
    email = safe.replace("_c_", ":").replace("_a_", "@").replace("__", "_")
    if unblock_email(DB_PATH, email):
        await cb.answer("↩️ Почта разблокирована", show_alert=True)
    else:
        await cb.answer("Не найдено", show_alert=True)
    text, kb = _render_emails_list(page)
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data.startswith("emails_del_"))
async def cb_emails_delete(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    parts = cb.data.replace("emails_del_", "").split("_", 1)
    page = int(parts[0]) if parts and parts[0].isdigit() else 0
    safe = parts[1] if len(parts) > 1 else ""
    email = safe.replace("_c_", ":").replace("_a_", "@").replace("__", "_")
    if delete_email(DB_PATH, email):
        await cb.answer("🗑 Удалено", show_alert=True)
    else:
        await cb.answer("Не найдено", show_alert=True)
    if page > 0 and get_emails_count(DB_PATH) <= page * 15:
        page = max(0, page - 1)
    text, kb = _render_emails_list(page)
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "emails_upload")
async def cb_emails_upload(cb: CallbackQuery, state: FSMContext) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    await state.clear()
    await cb.message.edit_text(
        "📤 <b>Загрузить CSV</b>\n\n"
        "Только Gmail. Колонки: email, apppassword\n"
        "(или mail/почта и password/пароль)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К меню почт", callback_data="admin_emails")],
        ]),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "emails_test")
async def cb_emails_test(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    from .database import get_random_email
    from .config import TEST_MAIL
    creds = get_random_email(DB_PATH)
    if not creds:
        await cb.answer("Нет доступных почт", show_alert=True)
        return
    await cb.answer("Отправляю тест...")
    email, password = creds
    ok = send_test_email(DB_PATH, email, password)
    if ok:
        await cb.bot.send_message(
            cb.message.chat.id,
            f"✅ Тест почты OK\n\nОтправлено с <code>{email}</code> на {TEST_MAIL}",
            parse_mode="HTML",
        )
    else:
        await cb.bot.send_message(
            cb.message.chat.id,
            f"❌ Ошибка отправки с <code>{email}</code>\n\nПочта помечена как заблокированная.",
            parse_mode="HTML",
        )


@router.callback_query(F.data == "emails_test_all")
async def cb_emails_test_all(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    from .config import TEST_MAIL
    if get_emails_count(DB_PATH) == 0:
        await cb.answer("Нет почт для теста", show_alert=True)
        return
    await cb.answer("Тестирую все почты...")
    ok_count, failed_count, failed_emails = test_all_emails(DB_PATH)
    lines = [
        f"🔄 <b>Тест всех почт</b> (на {TEST_MAIL})",
        "",
        f"✅ Работают: {ok_count}",
        f"❌ Не работают: {failed_count}",
    ]
    if failed_emails:
        lines.append("")
        lines.append("Заблокированы:")
        for e in failed_emails[:10]:
            lines.append(f"• <code>{e}</code>")
        if len(failed_emails) > 10:
            lines.append(f"... и ещё {len(failed_emails) - 10}")
    await cb.bot.send_message(
        cb.message.chat.id,
        "\n".join(lines),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "emails_export")
async def cb_emails_export(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    rows = get_emails(DB_PATH, limit=10000)
    if not rows:
        await cb.answer("Нет почт для экспорта", show_alert=True)
        return
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["email", "password"])
    for email, password, *_ in rows:
        w.writerow([email, password])
    buf.seek(0)
    data = buf.getvalue().encode("utf-8-sig")
    doc = BufferedInputFile(data, filename="emails_export.csv")
    await cb.bot.send_document(cb.message.chat.id, document=doc, caption=f"📥 Экспорт: {len(rows)} почт")
    await cb.answer("📥 Файл отправлен")


# --- Шаблоны ---
def _template_vars_help() -> str:
    lines = ["<b>Доступные переменные:</b>\n"]
    for var, desc in TEMPLATE_VARS.items():
        lines.append(f"• <code>{{{var}}}</code> — {desc}")
    return "\n".join(lines)


def _template_example() -> str:
    return (
        "Привет! Меня зовут {user_name}.\n"
        "Хотела бы купить ваш товар «{title}» ({price}).\n"
        "Ссылка: {url}\n\nС уважением."
    )


def _render_templates() -> tuple[str, InlineKeyboardMarkup]:
    templates = get_templates(DB_PATH)
    active_id = get_active_template_id(DB_PATH)
    if not templates:
        text = "📝 <b>Шаблоны сообщений</b>\n\nНет шаблонов."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить", callback_data="tpl_add")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_main")],
        ])
        return text, kb
    lines = ["📝 <b>Шаблоны</b>\n"]
    btns = []
    for tid, name, body, _ in templates:
        preview = (body or "")[:50] + "…" if len(body or "") > 50 else (body or "")
        active_badge = " ✅ активен" if tid == active_id else ""
        lines.append(f"• <b>{html.escape(name)}</b>{active_badge}\n  <i>{html.escape(preview)}</i>")
        btns.append([
            InlineKeyboardButton(text=f"✓ Активен" if tid == active_id else f"▶️ Выбрать", callback_data=f"tpl_activate_{tid}"),
            InlineKeyboardButton(text=f"✏️", callback_data=f"tpl_edit_{tid}"),
            InlineKeyboardButton(text=f"🗑", callback_data=f"tpl_del_{tid}"),
        ])
    btns.append([InlineKeyboardButton(text="➕ Добавить", callback_data="tpl_add")])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_main")])
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3997] + "..."
    return text, InlineKeyboardMarkup(inline_keyboard=btns)


@router.callback_query(F.data == "admin_templates")
async def cb_admin_templates(cb: CallbackQuery, state: FSMContext) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    await state.clear()
    text, kb = _render_templates()
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data == "tpl_add")
async def cb_tpl_add(cb: CallbackQuery, state: FSMContext) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    await state.set_state(TemplateState.name)
    help_text = _template_vars_help()
    example = _template_example()
    example_filled = format_template_example(example)
    await cb.message.edit_text(
        f"📝 <b>Новый шаблон</b>\n\n"
        f"Шаг 1/2: Введите <b>название</b> шаблона (например: «Покупка»)\n\n"
        f"{help_text}\n\n"
        f"<b>Пример шаблона:</b>\n<pre>{html.escape(example)}</pre>\n\n"
        f"<b>Пример с подставленными значениями:</b>\n<pre>{html.escape(example_filled)}</pre>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_templates")],
        ]),
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(TemplateState.name, F.text)
async def msg_tpl_name(msg: Message, state: FSMContext) -> None:
    if str(msg.chat.id) != str(ADMIN_CHAT_ID):
        return
    name = (msg.text or "").strip()
    if not name:
        await msg.answer("Введите название")
        return
    await state.update_data(tpl_name=name)
    await state.set_state(TemplateState.body)
    await msg.answer(
        f"Шаг 2/2: Введите <b>текст шаблона</b>.\n\n"
        "Используйте переменные: " + ", ".join(f"<code>{{{v}}}</code>" for v in TEMPLATE_VARS),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_templates")],
        ]),
        parse_mode="HTML",
    )


@router.message(TemplateState.body, F.text)
async def msg_tpl_body(msg: Message, state: FSMContext) -> None:
    if str(msg.chat.id) != str(ADMIN_CHAT_ID):
        return
    body = msg.text or ""
    data = await state.get_data()
    edit_id = data.get("tpl_edit_id")
    if edit_id:
        tpl = get_template(DB_PATH, edit_id)
        name = tpl[0] if tpl else "Шаблон"
        update_template(DB_PATH, edit_id, name, body)
        await msg.answer(f"✅ Шаблон «{name}» обновлён")
    else:
        name = data.get("tpl_name", "Без названия")
        add_template(DB_PATH, name, body)
        await msg.answer(f"✅ Шаблон «{name}» добавлен")
    await state.clear()
    text, kb = _render_templates()
    await msg.answer(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data.startswith("tpl_activate_"))
async def cb_tpl_activate(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    try:
        tid = int(cb.data.replace("tpl_activate_", ""))
    except ValueError:
        await cb.answer()
        return
    tpl = get_template(DB_PATH, tid)
    if not tpl:
        await cb.answer("Шаблон не найден", show_alert=True)
        return
    set_active_template_id(DB_PATH, tid)
    text, kb = _render_templates()
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cb.answer("✅ Шаблон активирован", show_alert=True)


@router.callback_query(F.data.startswith("tpl_edit_"))
async def cb_tpl_edit(cb: CallbackQuery, state: FSMContext) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    try:
        tid = int(cb.data.replace("tpl_edit_", ""))
    except ValueError:
        await cb.answer()
        return
    tpl = get_template(DB_PATH, tid)
    if not tpl:
        await cb.answer("Не найден", show_alert=True)
        return
    name, body = tpl
    await state.update_data(tpl_edit_id=tid)
    await state.set_state(TemplateState.body)
    await cb.message.edit_text(
        f"✏️ Редактирование «{name}»\n\n"
        f"Отправьте новый текст шаблона:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_templates")],
        ]),
    )
    await cb.message.answer(f"<pre>{html.escape(body)}</pre>", parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data.startswith("tpl_del_"))
async def cb_tpl_delete(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    try:
        tid = int(cb.data.replace("tpl_del_", ""))
    except ValueError:
        await cb.answer()
        return
    if delete_template(DB_PATH, tid):
        if get_active_template_id(DB_PATH) == tid:
            set_active_template_id(DB_PATH, None)
        text, kb = _render_templates()
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await cb.answer("🗑 Шаблон удалён", show_alert=True)
    else:
        await cb.answer("Не найден", show_alert=True)


@router.callback_query(F.data == "admin_main")
async def cb_admin_main(cb: CallbackQuery, state: FSMContext) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    await state.clear()
    await cb.message.edit_text(
        "👑 <b>Панель администратора</b>\n\nВыберите действие:",
        reply_markup=_admin_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


def run_admin_bot() -> None:
    if not ADMIN_BOT_TOKEN:
        print("Установите ADMIN_BOT_TOKEN в .env")
        return
    init_db(DB_PATH)
    dp = Dispatcher()
    dp.include_router(router)
    bot = Bot(token=ADMIN_BOT_TOKEN)

    async def main() -> None:
        await dp.start_polling(bot)

    asyncio.run(main())


if __name__ == "__main__":
    run_admin_bot()
