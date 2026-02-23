#!/usr/bin/env python3
"""Telegram бот для Marktplaats — aiogram 3, асинхронный."""
import asyncio
import logging
import queue
import threading
from datetime import datetime
from queue import Empty
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

from .config import AUTH_CODE, ADMIN_CHAT_ID, BOT_TOKEN, DATA_DIR, HEADLESS, DB_PATH, LISTINGS_CSV, SCRAPER_DIR
from .database import (
    init_db,
    sync_csv_to_db,
    authorize_user,
    is_authorized,
    register_pending_user,
    get_listings,
    get_last_update_date,
    get_categories,
    export_listings_to_csv,
    get_listings_count,
    SORT_FIELDS,
)

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router()

# Состояния
class AuthState(StatesGroup):
    code = State()

class ParsingState(StatesGroup):
    workers = State()
    limit = State()

class FiltersState(StatesGroup):
    category = State()
    price = State()
    limit = State()
    date = State()
    sort = State()

USER_FILTERS: dict[int, dict] = {}
PARSING_RUNNING = False
PARSING_CHAT_ID: int | None = None
PARSING_LIMIT: int = 0  # целевое количество (initial + limit)
PARSING_INITIAL_COUNT: int = 0  # сколько было в БД на старте
PARSING_USER_LIMIT: int = 0  # лимит пользователя (сколько новых собрать)

WATCH_RUNNING = False
WATCH_CHAT_ID: int | None = None
WATCH_QUEUE: queue.Queue = queue.Queue()
WATCH_STOP_EVENT = threading.Event()
WATCH_THREAD: threading.Thread | None = None


async def _progress_updater(
    bot: Bot, chat_id: int, message_id: int, limit_val: int, initial_count: int = 0
) -> None:
    """Обновляет статус каждые 2 сек (новые товары из БД)."""
    interval = 2
    while PARSING_RUNNING:
        await asyncio.sleep(interval)
        if not PARSING_RUNNING:
            break
        count = get_listings_count(DB_PATH)
        new_count = max(0, count - initial_count)
        try:
            text = _main_text(
                progress_count=new_count,
                progress_limit=limit_val if limit_val > 0 else None,
            )
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=_main_kb(),
            )
        except Exception:
            pass


async def _stream_log(stream: asyncio.StreamReader | None) -> bytes:
    """Читает поток построчно и логирует. Возвращает весь вывод."""
    if stream is None:
        return b""
    buf = []
    while True:
        line = await stream.readline()
        if not line:
            break
        try:
            text = line.decode("utf-8", errors="replace").rstrip()
        except Exception:
            text = str(line)
        if text and not text.isspace():
            logger.info("[парсер] %s", text)
        buf.append(line)
    return b"".join(buf)


async def _run_parsing(workers: int, limit_val: int, chat_id: int, bot: Bot) -> None:
    global PARSING_RUNNING, PARSING_CHAT_ID, PARSING_LIMIT, PARSING_INITIAL_COUNT, PARSING_USER_LIMIT
    cmd = [
        "python", "-m", "mpscraper",
        "-d", DATA_DIR,
        "--db-path", DB_PATH,
        "-l", str(limit_val),
        "-w", str(workers),
        "--skip-count",
    ]
    if HEADLESS:
        cmd.append("--headless")
    logger.info("Парсинг: %s", " ".join(cmd))
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=SCRAPER_DIR,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_task = asyncio.create_task(_stream_log(proc.stdout))
        stderr_task = asyncio.create_task(_stream_log(proc.stderr))
        await asyncio.wait_for(asyncio.gather(stdout_task, stderr_task, proc.wait()), timeout=3600)
        stdout = stdout_task.result()
        stderr = stderr_task.result()
        if proc.returncode == 0:
            tail = (stdout + stderr).decode("utf-8", errors="replace")[-400:]
            await bot.send_message(chat_id, f"✅ Сбор данных завершён!\n\n{tail}")
        else:
            err = (stderr or stdout).decode("utf-8", errors="replace")
            await bot.send_message(chat_id, f"❌ Произошла ошибка\n\n{err[-400:]}")
    except asyncio.TimeoutError:
        if proc and proc.returncode is None:
            proc.kill()
        await bot.send_message(chat_id, "⏱ Превышено время ожидания\n\nПопробуйте уменьшить лимит объявлений")
    except Exception as e:
        logger.exception("Парсинг: %s", e)
        await bot.send_message(chat_id, f"❌ Ошибка: {e}")
    finally:
        PARSING_RUNNING = False
        PARSING_CHAT_ID = None
        PARSING_LIMIT = 0
        PARSING_INITIAL_COUNT = 0
        PARSING_USER_LIMIT = 0


def _plural(n: int, one: str, few: str, many: str) -> str:
    """Русская плюрализация: 1 товар, 2 товара, 5 товаров."""
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and (n % 100 < 10 or n % 100 >= 20):
        return few
    return many


def _main_text(
    progress_count: int | None = None,
    progress_limit: int | None = None,
) -> str:
    last = get_last_update_date(DB_PATH)
    last_str = last if last else "данных пока нет"
    count = get_listings_count(DB_PATH)
    count_str = _plural(count, "товар", "товара", "товаров")
    if PARSING_RUNNING:
        if progress_count is None:
            progress_count = max(0, count - PARSING_INITIAL_COUNT)
        if progress_limit is None:
            progress_limit = PARSING_USER_LIMIT if PARSING_USER_LIMIT > 0 else None
        if progress_limit and progress_limit > 0:
            pct = min(100, int(100 * progress_count / progress_limit))
            status = f"🟢 Идёт сбор данных... Новых: {progress_count} / {progress_limit} ({pct}%)"
        else:
            w = _plural(progress_count or 0, "товар", "товара", "товаров")
            status = f"🟢 Идёт сбор данных... Собрано {progress_count or 0} {w}"
    elif WATCH_RUNNING:
        status = f"👁 Слежение активно · новые объявления приходят сюда · 📦 {count} {count_str}"
    else:
        status = f"⚪ Готов к работе · 📦 {count} {count_str}"
    return f"🏠 Marktplaats — поиск товаров\n\n📅 Последнее обновление: {last_str}\n📊 {status}"


def _main_kb() -> InlineKeyboardMarkup:
    btns = [
        [InlineKeyboardButton(text="🔄 Собрать новые данные", callback_data="parse")],
        [InlineKeyboardButton(text="📋 Скачать товары", callback_data="listings")],
    ]
    if WATCH_RUNNING:
        btns.append([InlineKeyboardButton(text="⏹ Остановить слежение", callback_data="watch_stop")])
    else:
        btns.append([InlineKeyboardButton(text="👁 Следить за новыми (24/7)", callback_data="watch_start")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def _format_listing_for_telegram(item: dict) -> str:
    """Краткое сообщение об объявлении для отправки в Telegram."""
    title = (item.get("title") or "")[:200]
    price = item.get("price_cents")
    price_str = f"€{price / 100:.2f}" if price is not None and price > 0 else "Цена по запросу"
    url = item.get("listing_url") or ""
    city = item.get("city_name") or ""
    parts = [f"🆕 {title}", f"💰 {price_str}"]
    if city:
        parts.append(f"📍 {city}")
    if url:
        parts.append(url)
    return "\n".join(parts)


def _back_kb(to_main: bool = True) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_main" if to_main else "back_listings")],
    ])


# --- Категории (человеческие названия) ---
def _category_kb() -> InlineKeyboardMarkup:
    cats = get_categories(DB_PATH)
    btns = [[InlineKeyboardButton(text="🌐 Все категории", callback_data="cat_all")]]
    for slug, name in cats[:25]:
        label = (name[:40] + "…") if len(name) > 40 else name
        btns.append([InlineKeyboardButton(text=label, callback_data=f"cat_{slug}")])
    btns.append([InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


# --- Сортировка ---
def _sort_kb() -> InlineKeyboardMarkup:
    btns = []
    for key, label_desc, label_asc in SORT_FIELDS[:10]:
        btns.append([
            InlineKeyboardButton(text=label_desc, callback_data=f"sort_{key}_desc"),
            InlineKeyboardButton(text=label_asc, callback_data=f"sort_{key}_asc"),
        ])
    btns.append([InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=btns)


def _pending_user_text() -> str:
    """Сообщение для неавторизованного пользователя (клиентский режим)."""
    return (
        "📩 <b>Заявка на регистрацию отправлена администратору</b>\n\n"
        "────────────────────────────────\n"
        "📋 <b>Ваш статус:</b> <i>Не подтверждён</i>\n"
        "────────────────────────────────\n\n"
        "Ожидайте подтверждения. После одобрения заявки вы получите доступ к боту.\n\n"
        "Если у вас есть код доступа — напишите его в чат для входа."
    )


@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext) -> None:
    await state.clear()
    user_id = msg.from_user.id if msg.from_user else 0
    if is_authorized(DB_PATH, user_id):
        await msg.answer(_main_text(), reply_markup=_main_kb())
    else:
        register_pending_user(DB_PATH, user_id)
        # Уведомление администратору
        if ADMIN_CHAT_ID:
            try:
                u = msg.from_user
                name = (u.first_name or "") + (" " + (u.last_name or "") or "")
                username = f"@{u.username}" if u and u.username else "—"
                await msg.bot.send_message(
                    ADMIN_CHAT_ID,
                    f"📩 <b>Новая заявка на регистрацию</b>\n\n"
                    f"👤 {name or '—'}\n"
                    f"🆔 ID: <code>{user_id}</code>\n"
                    f"📱 {username}",
                    parse_mode="HTML",
                )
            except Exception:
                pass
        await msg.answer(_pending_user_text(), parse_mode="HTML")


@router.message(AuthState.code, F.text)
async def auth_code(msg: Message, state: FSMContext) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    if (msg.text or "").strip() == AUTH_CODE:
        authorize_user(DB_PATH, user_id)
        await state.clear()
        await msg.answer("✅ Авторизация успешна!\n\nДобро пожаловать в систему\n\n" + _main_text(), reply_markup=_main_kb())
    else:
        await msg.answer("❌ Неверный код\n\nПопробуйте ещё раз или обратитесь к администратору")


@router.callback_query(F.data == "back_main")
async def cb_back_main(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await cb.message.edit_text(_main_text(), reply_markup=_main_kb())
    await cb.answer()


async def _watch_queue_reader(bot: Bot) -> None:
    """Фоновый таск: читает очередь новых объявлений и отправляет в Telegram."""
    loop = asyncio.get_event_loop()
    while True:
        try:
            chat_id, item = await loop.run_in_executor(
                None, lambda: WATCH_QUEUE.get(timeout=2)
            )
            text = _format_listing_for_telegram(item)
            await bot.send_message(chat_id, text)
        except Empty:
            pass
        except Exception as e:
            logger.exception("watch queue: %s", e)
        await asyncio.sleep(0.1)


@router.callback_query(F.data == "watch_start")
async def cb_watch_start(cb: CallbackQuery, state: FSMContext) -> None:
    global WATCH_RUNNING, WATCH_CHAT_ID, WATCH_THREAD
    user_id = cb.from_user.id if cb.from_user else 0
    if not is_authorized(DB_PATH, user_id):
        await cb.answer("🔒 Сначала авторизуйтесь", show_alert=True)
        return
    if WATCH_RUNNING:
        await cb.answer("👁 Слежение уже запущено", show_alert=True)
        return
    if PARSING_RUNNING:
        await cb.answer("⏳ Дождитесь завершения сбора данных", show_alert=True)
        return
    WATCH_RUNNING = True
    WATCH_CHAT_ID = cb.message.chat.id
    WATCH_STOP_EVENT.clear()
    from .watch_runner import run_watch_loop
    WATCH_THREAD = threading.Thread(
        target=run_watch_loop,
        kwargs=dict(
            db_path=DB_PATH,
            chat_id=WATCH_CHAT_ID,
            new_listing_queue=WATCH_QUEUE,
            stop_event=WATCH_STOP_EVENT,
            headless=HEADLESS,
        ),
        daemon=True,
    )
    WATCH_THREAD.start()
    await cb.message.edit_text(
        _main_text(),
        reply_markup=_main_kb(),
    )
    await cb.answer("👁 Слежение запущено! Новые объявления будут приходить сюда.")


@router.callback_query(F.data == "watch_stop")
async def cb_watch_stop(cb: CallbackQuery, state: FSMContext) -> None:
    global WATCH_RUNNING, WATCH_CHAT_ID, WATCH_THREAD
    if not WATCH_RUNNING:
        await cb.answer("Слежение не запущено", show_alert=True)
        return
    WATCH_STOP_EVENT.set()
    if WATCH_THREAD and WATCH_THREAD.is_alive():
        WATCH_THREAD.join(timeout=10)
    WATCH_RUNNING = False
    WATCH_CHAT_ID = None
    WATCH_THREAD = None
    await cb.message.edit_text(_main_text(), reply_markup=_main_kb())
    await cb.answer("⏹ Слежение остановлено")


@router.callback_query(F.data == "parse")
async def cb_parse(cb: CallbackQuery, state: FSMContext) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if not is_authorized(DB_PATH, user_id):
        await cb.answer("🔒 Сначала авторизуйтесь\n\nНажмите /start для входа", show_alert=True)
        return
    global PARSING_RUNNING
    if PARSING_RUNNING:
        await cb.answer("⏳ Сбор данных уже идёт, подождите завершения", show_alert=True)
        return
    await state.set_state(ParsingState.workers)
    await cb.message.edit_text(
        "⚙️ Шаг 1/2: Скорость сбора\n\nВведите число от 1 до 10\n\n• 1-3 — медленно, но надёжно\n• 4-6 — средняя скорость\n• 7-10 — быстро, но больше нагрузка",
        reply_markup=_back_kb(),
    )
    await cb.answer()


@router.message(ParsingState.workers, F.text)
async def parsing_workers(msg: Message, state: FSMContext) -> None:
    try:
        w = int((msg.text or "").strip())
        if 1 <= w <= 10:
            await state.update_data(workers=w)
            await state.set_state(ParsingState.limit)
            await msg.answer("📊 Шаг 2/2: Лимит объявлений\n\nСколько объявлений собрать?\n\n• Введите число (например: 100, 500)\n• Введите 0 — собрать всё без ограничений", reply_markup=_back_kb())
            return
    except ValueError:
        pass
    await msg.answer("❌ Введите число от 1 до 10")


@router.message(ParsingState.limit, F.text)
async def parsing_limit(msg: Message, state: FSMContext) -> None:
    try:
        limit = int((msg.text or "").strip())
        if limit >= 0:
            data = await state.get_data()
            workers = data.get("workers", 1)
            await state.clear()
            global PARSING_RUNNING, PARSING_CHAT_ID, PARSING_LIMIT, PARSING_INITIAL_COUNT, PARSING_USER_LIMIT
            PARSING_RUNNING = True
            PARSING_CHAT_ID = msg.chat.id
            initial_count = get_listings_count(DB_PATH)
            PARSING_INITIAL_COUNT = initial_count
            PARSING_LIMIT = initial_count + limit if limit > 0 else 0
            PARSING_USER_LIMIT = limit if limit > 0 else 0
            await msg.answer(
                f"✅ Сбор данных запущен!\n\n• Скорость: {workers} потоков\n• Лимит: {limit} (0 = без лимита)\n\n⏳ Я напишу, когда всё будет готово",
                reply_markup=_main_kb(),
            )
            status_msg = await msg.answer(
                _main_text(
                    progress_count=0,
                    progress_limit=limit if limit > 0 else None,
                ),
                reply_markup=_main_kb(),
            )
            asyncio.create_task(
                _progress_updater(
                    msg.bot, msg.chat.id, status_msg.message_id, limit, initial_count
                )
            )
            asyncio.create_task(_run_parsing(workers, limit, msg.chat.id, msg.bot))
            return
    except ValueError:
        pass
    await msg.answer("❌ Введите число 0 или больше")


# --- Получить товары ---
@router.callback_query(F.data == "listings")
async def cb_listings(cb: CallbackQuery, state: FSMContext) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    if not is_authorized(DB_PATH, user_id):
        await cb.answer("🔒 Сначала авторизуйтесь\n\nНажмите /start для входа", show_alert=True)
        return
    USER_FILTERS[user_id] = {
        "_step": 1,
        "category_slug": None,
        "price_min": None,
        "price_max": None,
        "limit": 25,
        "min_date": None,
        "sort_by": "listed_timestamp",
        "sort_desc": True,
    }
    await cb.message.edit_text(
        "📂 Шаг 1/4: Категория\n\nВыберите категорию товаров:",
        reply_markup=_category_kb(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("cat_"))
async def cb_category(cb: CallbackQuery, state: FSMContext) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    slug = None if cb.data == "cat_all" else cb.data.replace("cat_", "")
    USER_FILTERS.setdefault(user_id, {})["category_slug"] = slug
    USER_FILTERS.setdefault(user_id, {})["_step"] = 2
    await cb.message.edit_text(
        "💰 Шаг 2/4: Фильтр по цене\n\nВведите диапазон цен в евро:\n\nПримеры:\n• 50 200 — от 50€ до 200€\n• 0 100 — до 100€\n• 500 9999 — от 500€\n\nИли нажмите кнопку ниже, чтобы пропустить ⬇️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить (любая цена)", callback_data="price_none")],
            [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_main")],
        ]),
    )
    await cb.answer()


@router.callback_query(F.data == "price_none")
async def cb_price_none(cb: CallbackQuery) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    USER_FILTERS.setdefault(user_id, {})["price_min"] = None
    USER_FILTERS.setdefault(user_id, {})["price_max"] = None
    USER_FILTERS.setdefault(user_id, {})["_step"] = 3
    await cb.message.edit_text(
        "📊 Шаг 3/4: Количество\n\nСколько товаров показать?\n\nВыберите или введите число от 1 до 100:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="10", callback_data="lim_10")],
            [InlineKeyboardButton(text="25", callback_data="lim_25")],
            [InlineKeyboardButton(text="50", callback_data="lim_50")],
            [InlineKeyboardButton(text="100", callback_data="lim_100")],
            [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_main")],
        ]),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("lim_"))
async def cb_limit(cb: CallbackQuery) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    n = int(cb.data.replace("lim_", ""))
    USER_FILTERS.setdefault(user_id, {})["limit"] = n
    USER_FILTERS.setdefault(user_id, {})["_step"] = 4
    await cb.message.edit_text(
        "📋 Шаг 4/4: Сортировка\n\nКак отсортировать результаты?",
        reply_markup=_sort_kb(),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("sort_"))
async def cb_sort(cb: CallbackQuery) -> None:
    user_id = cb.from_user.id if cb.from_user else 0
    parts = cb.data.replace("sort_", "").split("_")
    if len(parts) >= 2:
        sort_by = parts[0]
        sort_desc = parts[1] == "desc"
        USER_FILTERS.setdefault(user_id, {})["sort_by"] = sort_by
        USER_FILTERS.setdefault(user_id, {})["sort_desc"] = sort_desc
    await _do_send_listings(cb, user_id)
    await cb.answer()


async def _do_send_listings(cb: CallbackQuery, user_id: int) -> None:
    f = USER_FILTERS.get(user_id, {})
    items = get_listings(
        DB_PATH,
        limit=f.get("limit", 25),
        min_date=f.get("min_date"),
        category_slug=f.get("category_slug"),
        min_price_cents=f.get("price_min"),
        max_price_cents=f.get("price_max"),
        sort_by=f.get("sort_by", "listed_timestamp"),
        sort_desc=f.get("sort_desc", True),
    )
    if not items:
        await cb.message.edit_text(
            "📭 Товары не найдены\n\nВозможные причины:\n• Данные ещё не собраны\n• Нет товаров по вашим фильтрам\n\n👉 Попробуйте сначала собрать данные",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_main")],
            ]),
        )
        return
    csv_bytes = export_listings_to_csv(items)
    fname = f"listings_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    await cb.message.edit_text("✅ Файл готов!")
    doc = BufferedInputFile(csv_bytes, filename=fname)
    await cb.bot.send_document(
        chat_id=cb.message.chat.id,
        document=doc,
        caption=f"📦 Найдено товаров: {len(items)}",
    )
    await cb.bot.send_message(
        chat_id=cb.message.chat.id,
        text=_main_text(),
        reply_markup=_main_kb(),
    )


@router.message(F.text)
async def handle_text(msg: Message, state: FSMContext) -> None:
    user_id = msg.from_user.id if msg.from_user else 0
    text = (msg.text or "").strip()
    current = await state.get_state()

    # Код доступа можно ввести в любой момент (для неавторизованных)
    if not is_authorized(DB_PATH, user_id) and text == AUTH_CODE:
        authorize_user(DB_PATH, user_id)
        await state.clear()
        await msg.answer("✅ Авторизация успешна!\n\nДобро пожаловать в систему\n\n" + _main_text(), reply_markup=_main_kb())
        return

    if current == AuthState.code:
        return  # обработано в auth_code

    if current == ParsingState.workers:
        return  # обработано в parsing_workers

    if current == ParsingState.limit:
        return  # обработано в parsing_limit

    # Фильтр цены в мастере товаров (шаг 2)
    f = USER_FILTERS.get(user_id, {})
    if f.get("_step") == 2:
        pmin, pmax = None, None
        if text.lower() not in ("нет", "no", "n", "-", ""):
            parts = text.split()
            try:
                if len(parts) >= 2:
                    a, b = float(parts[0]), float(parts[1])
                    if a > 0:
                        pmin = int(a * 100)
                    if b > 0:
                        pmax = int(b * 100)
                elif len(parts) == 1:
                    v = float(parts[0])
                    if v > 0:
                        pmin = pmax = int(v * 100)
            except ValueError:
                await msg.answer(
                    "❌ Не понял формат цены\n\nВведите два числа через пробел:\n"
                    "• Первое — минимальная цена\n• Второе — максимальная цена\n\nПример: 50 200",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="⏭ Пропустить (любая цена)", callback_data="price_none")],
                        [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_main")],
                    ]),
                )
                return
        USER_FILTERS.setdefault(user_id, {})["price_min"] = pmin
        USER_FILTERS.setdefault(user_id, {})["price_max"] = pmax
        USER_FILTERS.setdefault(user_id, {})["_step"] = 3
        await msg.answer(
            "📊 Шаг 3/4: Количество\n\nСколько товаров показать?\n\nВыберите или введите число от 1 до 100:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="10", callback_data="lim_10")],
                [InlineKeyboardButton(text="25", callback_data="lim_25")],
                [InlineKeyboardButton(text="50", callback_data="lim_50")],
                [InlineKeyboardButton(text="100", callback_data="lim_100")],
                [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_main")],
            ]),
        )
        return

    # Неавторизованный пользователь — показываем статус заявки
    if not is_authorized(DB_PATH, user_id):
        await msg.answer(_pending_user_text(), parse_mode="HTML")
        return

    # Количество в мастере товаров (шаг 3)
    if f.get("_step") == 3:
        try:
            n = int(text)
            if 1 <= n <= 100:
                USER_FILTERS.setdefault(user_id, {})["limit"] = n
                USER_FILTERS.setdefault(user_id, {})["_step"] = 4
                await msg.answer(
                    "📋 Шаг 4/4: Сортировка\n\nКак отсортировать результаты?",
                    reply_markup=_sort_kb(),
                )
                return
        except ValueError:
            pass
        await msg.answer(
            "❌ Введите число от 1 до 100\n\nИли выберите кнопку выше ⬆️",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="10", callback_data="lim_10")],
                [InlineKeyboardButton(text="25", callback_data="lim_25")],
                [InlineKeyboardButton(text="50", callback_data="lim_50")],
                [InlineKeyboardButton(text="100", callback_data="lim_100")],
                [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="back_main")],
            ]),
        )
        return

    await msg.answer(_main_text(), reply_markup=_main_kb())


def run_bot() -> None:
    if not BOT_TOKEN:
        print("Установите BOT_TOKEN в .env")
        return
    init_db(DB_PATH)
    # Однократная миграция: если есть старый CSV и БД пуста — импортируем
    if get_listings_count(DB_PATH) == 0 and Path(LISTINGS_CSV).exists():
        sync_csv_to_db(LISTINGS_CSV, DB_PATH)
    dp = Dispatcher()
    dp.include_router(router)
    bot = Bot(token=BOT_TOKEN)

    async def main() -> None:
        asyncio.create_task(_watch_queue_reader(bot))
        await dp.start_polling(bot)

    asyncio.run(main())


if __name__ == "__main__":
    run_bot()
