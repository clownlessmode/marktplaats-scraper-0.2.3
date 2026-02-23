#!/usr/bin/env python3
"""Клиентский бот для воркеров: /start → заявка, смена, товары сегодня, почты, шаблоны."""
import asyncio
import html
import logging
from pathlib import Path

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from .config import ADMIN_CHAT_ID, ADMIN_BOT_TOKEN, CLIENT_BOT_TOKEN, DB_PATH
from .database import (
    init_db,
    is_authorized,
    is_blocked,
    is_shift_active,
    register_pending_user,
    authorize_user,
    set_shift_active,
    get_worker_listings_today,
    get_emails,
    get_emails_count,
    add_emails_batch,
    parse_emails_text,
    parse_emails_csv,
    add_template,
    get_templates,
    get_template,
    update_template,
    delete_template,
    get_active_template_id,
    set_active_template_id,
    format_template_example,
    TEMPLATE_VARS,
)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router()


class WorkerEmailsState(StatesGroup):
    add_text = State()


class WorkerTemplateState(StatesGroup):
    name = State()
    body = State()


# Очередь для уведомлений админу о новых воркерах (отправляем с кнопками через клиентский бот)
PENDING_NOTIFICATIONS: dict[int, int] = {}  # user_id -> message_id (для обновления после approve/reject)


def _pending_text() -> str:
    return (
        "📩 <b>Заявка на регистрацию отправлена администратору</b>\n\n"
        "────────────────────────────────\n"
        "📋 <b>Ваш статус:</b> <i>Не подтверждён</i>\n"
        "────────────────────────────────\n\n"
        "Ожидайте подтверждения. Администратор одобрит или отклонит заявку.\n\n"
        "Если заявка отклонена — бот больше не будет отвечать."
    )


def _worker_kb(on_shift: bool) -> InlineKeyboardMarkup:
    base_btns = [
        [InlineKeyboardButton(text="📦 Товары сегодня", callback_data="list_today")],
        [InlineKeyboardButton(text="📧 Почты", callback_data="worker_emails")],
        [InlineKeyboardButton(text="📝 Шаблоны", callback_data="worker_templates")],
    ]
    if on_shift:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛑 Закрыть смену", callback_data="shift_stop")],
            *base_btns,
        ])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Начать смену", callback_data="shift_start")],
        *base_btns,
    ])


@router.message(CommandStart())
async def cmd_start(msg: Message) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    if is_blocked(DB_PATH, user_id):
        return  # не отвечаем заблокированным
    if is_authorized(DB_PATH, user_id):
        on_shift = is_shift_active(DB_PATH, user_id)
        await msg.answer(
            "👋 Добро пожаловать!\n\n"
            "Начните смену, чтобы получать уведомления о новых товарах (&lt; 3 ч).",
            reply_markup=_worker_kb(on_shift),
            parse_mode="HTML",
        )
    else:
        register_pending_user(DB_PATH, user_id)
        if ADMIN_CHAT_ID:
            try:
                u = msg.from_user
                name = (u.first_name or "") + (" " + (u.last_name or "") if u.last_name else "")
                username = f"@{u.username}" if u and u.username else "—"
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_{user_id}"),
                        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{user_id}"),
                    ],
                ])
                text = (
                    f"📩 <b>Новый воркер</b>\n\n"
                    f"👤 {name or '—'}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"📱 {username}"
                )
                if ADMIN_BOT_TOKEN:
                    admin_bot = Bot(token=ADMIN_BOT_TOKEN)
                    async with admin_bot.context():
                        await admin_bot.send_message(
                            ADMIN_CHAT_ID, text, reply_markup=kb, parse_mode="HTML"
                        )
                else:
                    await msg.bot.send_message(
                        ADMIN_CHAT_ID, text, reply_markup=kb, parse_mode="HTML"
                    )
            except Exception as e:
                logger.exception("Notify admin: %s", e)
        await msg.answer(_pending_text(), parse_mode="HTML")


@router.callback_query(F.data.startswith("approve_"))
async def cb_approve(cb: CallbackQuery) -> None:
    if str(cb.message.chat.id) != str(ADMIN_CHAT_ID):
        await cb.answer()
        return
    try:
        uid = int(cb.data.replace("approve_", ""))
        authorize_user(DB_PATH, uid)
        try:
            await cb.bot.send_message(
                uid,
                "✅ <b>Ваша заявка одобрена!</b>\n\n"
                "Теперь вы можете пользоваться ботом. Нажмите /start для начала.",
                parse_mode="HTML",
            )
        except Exception:
            pass
        await cb.message.edit_reply_markup(reply_markup=None)
        await cb.message.edit_text(cb.message.text + "\n\n✅ Одобрено")
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
        from .database import block_user
        block_user(DB_PATH, uid)
        await cb.message.edit_reply_markup(reply_markup=None)
        await cb.message.edit_text(cb.message.text + "\n\n❌ Отклонён и заблокирован")
        await cb.answer("❌ Воркер отклонён и заблокирован", show_alert=True)
    except Exception as e:
        await cb.answer(f"Ошибка: {e}", show_alert=True)


@router.callback_query(F.data == "shift_start")
async def cb_shift_start(cb: CallbackQuery) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id):
        await cb.answer()
        return
    if not is_authorized(DB_PATH, user_id):
        await cb.answer("🔒 Сначала дождитесь одобрения", show_alert=True)
        return
    set_shift_active(DB_PATH, user_id, True)
    await cb.message.edit_text(
        "🟢 <b>Смена начата</b>\n\n"
        "Вы будете получать уведомления о новых товарах (&lt; 3 ч).",
        reply_markup=_worker_kb(True),
        parse_mode="HTML",
    )
    await cb.answer("🟢 Смена начата")


@router.callback_query(F.data == "shift_stop")
async def cb_shift_stop(cb: CallbackQuery) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id):
        await cb.answer()
        return
    set_shift_active(DB_PATH, user_id, False)
    await cb.message.edit_text(
        "⚪ <b>Смена закрыта</b>\n\n"
        "Уведомления приостановлены.",
        reply_markup=_worker_kb(False),
        parse_mode="HTML",
    )
    await cb.answer("⚪ Смена закрыта")


@router.callback_query(F.data == "list_today")
async def cb_list_today(cb: CallbackQuery) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id):
        await cb.answer()
        return
    items = get_worker_listings_today(DB_PATH, user_id)
    if not items:
        await cb.answer("📦 Сегодня товаров нет", show_alert=True)
        return
    lines = [f"📦 <b>Товары сегодня ({len(items)})</b>\n"]
    for i, it in enumerate(items[:25], 1):
        price = f"€{it['price_cents']/100:.2f}" if it.get("price_cents") else "—"
        lines.append(f"{i}. {it['title'][:50]} — {price}")
        if it.get("listing_url"):
            lines.append(f"   {it['listing_url']}")
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3997] + "..."
    await cb.message.answer(text, parse_mode="HTML")
    await cb.answer()


# --- Меню почт для воркеров ---
def _worker_emails_kb() -> InlineKeyboardMarkup:
    count = get_emails_count(DB_PATH)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить (mail:apppassword)", callback_data="worker_emails_add")],
        [InlineKeyboardButton(text="📤 Загрузить CSV", callback_data="worker_emails_upload")],
        [InlineKeyboardButton(text=f"📋 Список ({count})", callback_data="worker_emails_list_0")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="worker_main")],
    ])


@router.callback_query(F.data == "worker_emails")
async def cb_worker_emails(cb: CallbackQuery, state: FSMContext) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        await cb.answer()
        return
    await state.clear()
    count = get_emails_count(DB_PATH)
    await cb.message.edit_text(
        f"📧 <b>База почт</b>\n\nВсего: {count}\n\n"
        "• Добавить — введите mail:apppassword (только Gmail, через Enter — несколько)\n"
        "• Загрузить CSV — пришлите файл .csv\n"
        "• Список — просмотр почт",
        reply_markup=_worker_emails_kb(),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data == "worker_main")
async def cb_worker_main(cb: CallbackQuery, state: FSMContext) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id):
        await cb.answer()
        return
    await state.clear()
    if is_authorized(DB_PATH, user_id):
        on_shift = is_shift_active(DB_PATH, user_id)
        await cb.message.edit_text(
            "👋 Добро пожаловать!\n\n"
            "Начните смену, чтобы получать уведомления о новых товарах (&lt; 3 ч).",
            reply_markup=_worker_kb(on_shift),
            parse_mode="HTML",
        )
    else:
        await cb.message.edit_text(_pending_text(), parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data == "worker_emails_add")
async def cb_worker_emails_add(cb: CallbackQuery, state: FSMContext) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        await cb.answer()
        return
    await state.set_state(WorkerEmailsState.add_text)
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
            [InlineKeyboardButton(text="❌ Отмена", callback_data="worker_emails")],
        ]),
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(WorkerEmailsState.add_text, F.text)
async def msg_worker_emails_add_text(msg: Message, state: FSMContext) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        return
    pairs = parse_emails_text(msg.text or "")
    if not pairs:
        await msg.answer("❌ Не найдено валидных строк. Формат: mail@gmail.com:apppassword")
        return
    added, skipped = add_emails_batch(DB_PATH, pairs)
    await state.clear()
    await msg.answer(f"✅ Добавлено: {added}, пропущено (дубли): {skipped}")
    await msg.answer("📧 База почт", reply_markup=_worker_emails_kb())


@router.callback_query(F.data == "worker_emails_upload")
async def cb_worker_emails_upload(cb: CallbackQuery, state: FSMContext) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        await cb.answer()
        return
    await state.clear()
    await cb.message.edit_text(
        "📤 <b>Загрузить CSV</b>\n\n"
        "Только Gmail. Колонки: email, apppassword\n"
        "(или mail/почта и password/пароль)\n\n"
        "Пришлите файл .csv",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К меню почт", callback_data="worker_emails")],
        ]),
        parse_mode="HTML",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("worker_emails_list_"))
async def cb_worker_emails_list(cb: CallbackQuery) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        await cb.answer()
        return
    try:
        page = int(cb.data.replace("worker_emails_list_", ""))
    except ValueError:
        page = 0
    per_page = 15
    offset = page * per_page
    rows = get_emails(DB_PATH, limit=per_page, offset=offset)
    total = get_emails_count(DB_PATH)
    if not rows:
        text = "📋 <b>Список почт</b>\n\nПусто."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К меню почт", callback_data="worker_emails")],
        ])
    else:
        lines = [f"📋 <b>Почты</b> (стр. {page + 1}, всего {total})\n"]
        for email, _, _, blocked in rows:
            lines.append(f"• <code>{email}</code>" + (" 🚫" if blocked else ""))
        btns = []
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"worker_emails_list_{page - 1}"))
        if offset + len(rows) < total:
            nav.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"worker_emails_list_{page + 1}"))
        if nav:
            btns.append(nav)
        btns.append([InlineKeyboardButton(text="◀️ К меню почт", callback_data="worker_emails")])
        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:3997] + "..."
        kb = InlineKeyboardMarkup(inline_keyboard=btns)
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cb.answer()


@router.message(F.document)
async def msg_worker_emails_csv(msg: Message, state: FSMContext) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
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
        await state.clear()
        await msg.answer(f"✅ Из CSV добавлено: {added}, пропущено (дубли): {skipped}")
        await msg.answer("📧 База почт", reply_markup=_worker_emails_kb())
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}")


# --- Шаблоны для воркеров ---
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


def _render_worker_templates() -> tuple[str, InlineKeyboardMarkup]:
    templates = get_templates(DB_PATH)
    active_id = get_active_template_id(DB_PATH)
    if not templates:
        text = "📝 <b>Шаблоны сообщений</b>\n\nНет шаблонов."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить", callback_data="worker_tpl_add")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="worker_main")],
        ])
        return text, kb
    lines = ["📝 <b>Шаблоны</b>\n"]
    btns = []
    for tid, name, body, _ in templates:
        preview = (body or "")[:50] + "…" if len(body or "") > 50 else (body or "")
        active_badge = " ✅ активен" if tid == active_id else ""
        lines.append(f"• <b>{html.escape(name)}</b>{active_badge}\n  <i>{html.escape(preview)}</i>")
        btns.append([
            InlineKeyboardButton(text="✓ Активен" if tid == active_id else "▶️ Выбрать", callback_data=f"worker_tpl_activate_{tid}"),
            InlineKeyboardButton(text="✏️", callback_data=f"worker_tpl_edit_{tid}"),
            InlineKeyboardButton(text="🗑", callback_data=f"worker_tpl_del_{tid}"),
        ])
    btns.append([InlineKeyboardButton(text="➕ Добавить", callback_data="worker_tpl_add")])
    btns.append([InlineKeyboardButton(text="◀️ Назад", callback_data="worker_main")])
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3997] + "..."
    return text, InlineKeyboardMarkup(inline_keyboard=btns)


@router.callback_query(F.data == "worker_templates")
async def cb_worker_templates(cb: CallbackQuery, state: FSMContext) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        await cb.answer()
        return
    await state.clear()
    text, kb = _render_worker_templates()
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data == "worker_tpl_add")
async def cb_worker_tpl_add(cb: CallbackQuery, state: FSMContext) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        await cb.answer()
        return
    await state.set_state(WorkerTemplateState.name)
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
            [InlineKeyboardButton(text="❌ Отмена", callback_data="worker_templates")],
        ]),
        parse_mode="HTML",
    )
    await cb.answer()


@router.message(WorkerTemplateState.name, F.text)
async def msg_worker_tpl_name(msg: Message, state: FSMContext) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        return
    name = (msg.text or "").strip()
    if not name:
        await msg.answer("Введите название")
        return
    await state.update_data(tpl_name=name)
    await state.set_state(WorkerTemplateState.body)
    await msg.answer(
        f"Шаг 2/2: Введите <b>текст шаблона</b>.\n\n"
        "Используйте переменные: " + ", ".join(f"<code>{{{v}}}</code>" for v in TEMPLATE_VARS),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="worker_templates")],
        ]),
        parse_mode="HTML",
    )


@router.message(WorkerTemplateState.body, F.text)
async def msg_worker_tpl_body(msg: Message, state: FSMContext) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
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
    text, kb = _render_worker_templates()
    await msg.answer(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data.startswith("worker_tpl_activate_"))
async def cb_worker_tpl_activate(cb: CallbackQuery) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        await cb.answer()
        return
    try:
        tid = int(cb.data.replace("worker_tpl_activate_", ""))
    except ValueError:
        await cb.answer()
        return
    tpl = get_template(DB_PATH, tid)
    if not tpl:
        await cb.answer("Шаблон не найден", show_alert=True)
        return
    set_active_template_id(DB_PATH, tid)
    text, kb = _render_worker_templates()
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await cb.answer("✅ Шаблон активирован", show_alert=True)


@router.callback_query(F.data.startswith("worker_tpl_edit_"))
async def cb_worker_tpl_edit(cb: CallbackQuery, state: FSMContext) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        await cb.answer()
        return
    try:
        tid = int(cb.data.replace("worker_tpl_edit_", ""))
    except ValueError:
        await cb.answer()
        return
    tpl = get_template(DB_PATH, tid)
    if not tpl:
        await cb.answer("Не найден", show_alert=True)
        return
    name, body = tpl
    await state.update_data(tpl_edit_id=tid)
    await state.set_state(WorkerTemplateState.body)
    await cb.message.edit_text(
        f"✏️ Редактирование «{name}»\n\n"
        "Отправьте новый текст шаблона:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="worker_templates")],
        ]),
    )
    await cb.message.answer(f"<pre>{html.escape(body)}</pre>", parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data.startswith("worker_tpl_del_"))
async def cb_worker_tpl_delete(cb: CallbackQuery) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if is_blocked(DB_PATH, user_id) or not is_authorized(DB_PATH, user_id):
        await cb.answer()
        return
    try:
        tid = int(cb.data.replace("worker_tpl_del_", ""))
    except ValueError:
        await cb.answer()
        return
    if delete_template(DB_PATH, tid):
        if get_active_template_id(DB_PATH) == tid:
            set_active_template_id(DB_PATH, None)
        text, kb = _render_worker_templates()
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        await cb.answer("🗑 Шаблон удалён", show_alert=True)
    else:
        await cb.answer("Не найден", show_alert=True)


@router.message(F.text)
async def handle_any(msg: Message) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    if is_blocked(DB_PATH, user_id):
        return
    if is_authorized(DB_PATH, user_id):
        on_shift = is_shift_active(DB_PATH, user_id)
        await msg.answer("Выберите действие:", reply_markup=_worker_kb(on_shift))
    else:
        await msg.answer(_pending_text(), parse_mode="HTML")


def run_client_bot() -> None:
    if not CLIENT_BOT_TOKEN:
        print("Установите CLIENT_BOT_TOKEN (или BOT_TOKEN) в .env")
        return
    init_db(DB_PATH)
    dp = Dispatcher()
    dp.include_router(router)
    bot = Bot(token=CLIENT_BOT_TOKEN)

    async def main() -> None:
        await dp.start_polling(bot)

    asyncio.run(main())


if __name__ == "__main__":
    run_client_bot()
