"""SQLite база для бота. Только человекочитаемые поля."""
import csv
import io
import logging
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# Маппинг CSV (русские) -> DB (английские)
RU_TO_EN = {
    "id_объявления": "item_id",
    "id_продавца": "seller_id",
    "id_родительской_категории": "parent_category_id",
    "id_подкатегории": "child_category_id",
    "категории": "category_verticals",
    "тип_объявления": "ad_type",
    "название": "title",
    "описание": "description",
    "тип_цены": "price_type",
    "цена_центы": "price_cents",
    "типы": "types",
    "услуги": "services",
    "ссылка": "listing_url",
    "изображения": "image_urls",
    "город": "city_name",
    "страна": "country_code",
    "дата_публикации": "listed_timestamp",
    "дата_сбора": "crawled_timestamp",
    "просмотры": "view_count",
    "в_избранном": "favorited_count",
    "имя_продавца": "seller_name",
    "широта": "latitude",
    "долгота": "longitude",
    "расстояние_м": "distance_meters",
    "страна_название": "country_name",
    "приоритет_товара": "priority_product",
    "признаки": "traits",
    "описание_категории": "category_specific_description",
    "зарезервировано": "reserved",
    "nap_доступен": "nap_available",
    "срочность": "urgency_feature_active",
    "продавец_верифицирован": "is_verified",
    "сайт_продавца": "seller_website_url",
    "атрибуты_json": "attributes_json",
}

# Поля для сортировки: (ключ_бд, подпись_для_desc, подпись_для_asc)
SORT_FIELDS = [
    ("title", "🔤 По названию (А→Я)", "🔤 По названию (Я→А)"),
    ("price_cents", "💰 По цене (дороже)", "💰 По цене (дешевле)"),
    ("city_name", "🏙 По городу (А→Я)", "🏙 По городу (Я→А)"),
    ("country_code", "🌍 По стране (А→Я)", "🌍 По стране (Я→А)"),
    ("listed_timestamp", "📅 По дате (новые)", "📅 По дате (старые)"),
    ("crawled_timestamp", "🕐 По сбору (новые)", "🕐 По сбору (старые)"),
    ("view_count", "👁 По просмотрам (больше)", "👁 По просмотрам (меньше)"),
    ("favorited_count", "⭐ По избранному (больше)", "⭐ По избранному (меньше)"),
    ("seller_name", "👤 По продавцу (А→Я)", "👤 По продавцу (Я→А)"),
    ("distance_meters", "📍 По расстоянию (дальше)", "📍 По расстоянию (ближе)"),
    ("priority_product", "⚡ По приоритету (высокий)", "⚡ По приоритету (низкий)"),
    ("ad_type", "🏷 По типу (А→Я)", "🏷 По типу (Я→А)"),
]


def get_conn(db_path: str):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(db_path)


def init_db(db_path: str) -> None:
    conn = get_conn(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            authorized INTEGER DEFAULT 0,
            created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            item_id TEXT PRIMARY KEY,
            seller_id TEXT,
            parent_category_id INTEGER,
            child_category_id INTEGER,
            category_verticals TEXT,
            ad_type TEXT,
            title TEXT,
            description TEXT,
            price_type TEXT,
            price_cents INTEGER,
            types TEXT,
            services TEXT,
            listing_url TEXT,
            image_urls TEXT,
            city_name TEXT,
            country_code TEXT,
            listed_timestamp TEXT,
            crawled_timestamp TEXT,
            view_count INTEGER,
            favorited_count INTEGER,
            seller_name TEXT,
            latitude REAL,
            longitude REAL,
            distance_meters INTEGER,
            country_name TEXT,
            priority_product TEXT,
            traits TEXT,
            category_specific_description TEXT,
            reserved INTEGER,
            nap_available INTEGER,
            urgency_feature_active INTEGER,
            is_verified INTEGER,
            seller_website_url TEXT,
            attributes_json TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_date ON listings(listed_timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price_cents)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_city ON listings(city_name)")
    # blocked_users: заблокированные (бот не отвечает)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS blocked_users (
            user_id INTEGER PRIMARY KEY,
            blocked_at TEXT
        )
    """)
    # worker_listings: какой воркер получил какой товар (для отображения "сегодня")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS worker_listings (
            item_id TEXT,
            user_id INTEGER,
            received_at TEXT,
            PRIMARY KEY (item_id, user_id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_worker_listings_user ON worker_listings(user_id, received_at)")
    # rotation_state: последний воркер для round-robin
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rotation_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # emails: база почт (email:password), blocked=1 если не удалось отправить
    conn.execute("""
        CREATE TABLE IF NOT EXISTS emails (
            email TEXT PRIMARY KEY,
            password TEXT,
            created_at TEXT,
            blocked INTEGER DEFAULT 0
        )
    """)
    # email_templates: шаблоны сообщений для писем
    conn.execute("""
        CREATE TABLE IF NOT EXISTS email_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT
        )
    """)
    # Миграция: shift_active в users
    try:
        conn.execute("ALTER TABLE users ADD COLUMN shift_active INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    # Миграция: blocked в emails
    try:
        conn.execute("ALTER TABLE emails ADD COLUMN blocked INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    # Миграция: недостающие колонки в listings (seller_name и др.)
    for col, ctype in [
        ("seller_name", "TEXT"),
        ("latitude", "REAL"),
        ("longitude", "REAL"),
        ("distance_meters", "INTEGER"),
        ("country_name", "TEXT"),
        ("priority_product", "TEXT"),
        ("traits", "TEXT"),
        ("category_specific_description", "TEXT"),
        ("reserved", "INTEGER"),
        ("nap_available", "INTEGER"),
        ("urgency_feature_active", "INTEGER"),
        ("is_verified", "INTEGER"),
        ("seller_website_url", "TEXT"),
        ("attributes_json", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE listings ADD COLUMN {col} {ctype}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()


def sync_csv_to_db(csv_path: str, db_path: str) -> int:
    """Синхронизация CSV в БД (для обратной совместимости)."""
    if not Path(csv_path).exists():
        return 0
    init_db(db_path)
    df = pd.read_csv(csv_path)
    df = df.rename(columns={k: v for k, v in RU_TO_EN.items() if k in df.columns})
    return upsert_listings(df, db_path)


def _sanitize_string_for_sqlite(s) -> str | None:
    """Убирает surrogate-символы, вызывающие UnicodeEncodeError при записи в SQLite."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    if not isinstance(s, str):
        return str(s)
    return s.encode("utf-8", errors="replace").decode("utf-8")


def _serialize_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """Конвертирует tuple/list в строки для SQLite."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].apply(
                lambda x: "|".join(str(i) for i in x) if isinstance(x, (tuple, list)) else x
            )
            out[col] = out[col].apply(_sanitize_string_for_sqlite)
        if col in ("reserved", "nap_available", "urgency_feature_active", "is_verified"):
            out[col] = out[col].fillna(0).astype(int)
    return out


def upsert_listings(df: pd.DataFrame, db_path: str) -> int:
    """Вставляет/обновляет объявления в БД. Возвращает количество записей."""
    if df is None or len(df) == 0:
        return 0
    init_db(db_path)
    # Сериализуем tuple/list в строки перед слиянием
    df = _serialize_df_for_db(df)
    conn = get_conn(db_path)
    existing = pd.read_sql("SELECT * FROM listings", conn)
    conn.close()
    if len(existing) > 0:
        merged = pd.concat([existing, df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["item_id"], keep="last", ignore_index=True)
        if len(merged) < len(existing):
            logger.error(
                "upsert_listings: потеря данных! existing=%d, df=%d, merged=%d — не сохраняю",
                len(existing),
                len(df),
                len(merged),
            )
            raise ValueError(
                f"Слияние уменьшило число записей ({len(existing)} → {len(merged)}). "
                "Проверьте дубликаты item_id или целостность данных."
            )
    else:
        merged = df
    logger.debug("БД: %d → %d объявлений", len(existing), len(merged))
    conn = get_conn(db_path)
    merged.to_sql("listings", conn, if_exists="replace", index=False)
    n = len(merged)
    conn.close()
    return n


def load_listings_from_db(db_path: str) -> tuple[pd.DataFrame, set[str]]:
    """Загружает объявления из БД. Возвращает (DataFrame, set item_ids)."""
    if not Path(db_path).exists():
        return pd.DataFrame(), set()
    init_db(db_path)
    conn = get_conn(db_path)
    df = pd.read_sql("SELECT * FROM listings", conn)
    conn.close()
    item_ids: set[str] = set()
    if "item_id" in df.columns and len(df) > 0:
        item_ids = set(str(x) for x in df["item_id"].dropna())
    return df, item_ids


def get_listings_count(db_path: str) -> int:
    """Количество объявлений в БД."""
    if not Path(db_path).exists():
        return 0
    conn = get_conn(db_path)
    row = conn.execute("SELECT COUNT(*) FROM listings").fetchone()
    conn.close()
    return row[0] if row else 0


def register_pending_user(db_path: str, user_id: int) -> None:
    """Сохранить пользователя как ожидающего подтверждения (authorized=0)."""
    conn = get_conn(db_path)
    conn.execute(
        "INSERT OR IGNORE INTO users (user_id, authorized, created_at) VALUES (?, 0, ?)",
        (user_id, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def authorize_user(db_path: str, user_id: int) -> None:
    conn = get_conn(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO users (user_id, authorized, created_at, shift_active) VALUES (?, 1, ?, 0)",
            (user_id, datetime.utcnow().isoformat()),
        )
    except sqlite3.OperationalError:
        conn.execute(
            "INSERT OR REPLACE INTO users (user_id, authorized, created_at) VALUES (?, 1, ?)",
            (user_id, datetime.utcnow().isoformat()),
        )
    conn.commit()
    conn.close()


def is_authorized(db_path: str, user_id: int) -> bool:
    conn = get_conn(db_path)
    row = conn.execute("SELECT authorized FROM users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return row and row[0] == 1


def is_blocked(db_path: str, user_id: int) -> bool:
    conn = get_conn(db_path)
    row = conn.execute("SELECT 1 FROM blocked_users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return row is not None


def block_user(db_path: str, user_id: int) -> None:
    conn = get_conn(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO blocked_users (user_id, blocked_at) VALUES (?, ?)",
        (user_id, datetime.utcnow().isoformat()),
    )
    conn.execute("UPDATE users SET authorized = 0 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def unblock_user(db_path: str, user_id: int) -> None:
    conn = get_conn(db_path)
    conn.execute("DELETE FROM blocked_users WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def delete_user(db_path: str, user_id: int) -> bool:
    """Полностью удалить воркера из БД (users, blocked_users, worker_listings)."""
    conn = get_conn(db_path)
    conn.execute("DELETE FROM blocked_users WHERE user_id = ?", (user_id,))
    conn.execute("DELETE FROM worker_listings WHERE user_id = ?", (user_id,))
    cur = conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def set_shift_active(db_path: str, user_id: int, active: bool) -> None:
    conn = get_conn(db_path)
    conn.execute(
        "UPDATE users SET shift_active = ? WHERE user_id = ?",
        (1 if active else 0, user_id),
    )
    conn.commit()
    conn.close()


def is_shift_active(db_path: str, user_id: int) -> bool:
    conn = get_conn(db_path)
    row = conn.execute("SELECT shift_active FROM users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return row is not None and row[0] == 1


def get_workers_on_shift(db_path: str) -> list[int]:
    """Воркеры на смене (authorized=1, shift_active=1, не заблокированы)."""
    conn = get_conn(db_path)
    try:
        rows = conn.execute("""
            SELECT user_id FROM users
            WHERE authorized = 1 AND shift_active = 1
            AND user_id NOT IN (SELECT user_id FROM blocked_users)
            ORDER BY user_id
        """).fetchall()
    except sqlite3.OperationalError:
        rows = conn.execute("""
            SELECT user_id FROM users
            WHERE authorized = 1 AND user_id NOT IN (SELECT user_id FROM blocked_users)
            ORDER BY user_id
        """).fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_next_worker_for_listing(db_path: str) -> int | None:
    """Следующий воркер для round-robin. Возвращает user_id или None."""
    workers = get_workers_on_shift(db_path)
    if not workers:
        return None
    conn = get_conn(db_path)
    row = conn.execute("SELECT value FROM rotation_state WHERE key = 'last_worker_id'").fetchone()
    conn.close()
    last_id = int(row[0]) if row and row[0] else None
    if last_id is None or last_id not in workers:
        return workers[0]
    idx = workers.index(last_id)
    next_idx = (idx + 1) % len(workers)
    return workers[next_idx]


def set_last_worker_sent(db_path: str, user_id: int) -> None:
    conn = get_conn(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO rotation_state (key, value) VALUES ('last_worker_id', ?)",
        (str(user_id),),
    )
    conn.commit()
    conn.close()


def record_worker_listing(db_path: str, item_id: str, user_id: int) -> None:
    conn = get_conn(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO worker_listings (item_id, user_id, received_at) VALUES (?, ?, ?)",
        (item_id, user_id, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def get_worker_listings_today(db_path: str, user_id: int) -> list[dict]:
    """Товары, полученные воркером сегодня (UTC)."""
    conn = get_conn(db_path)
    rows = conn.execute("""
        SELECT wl.item_id, wl.received_at, l.title, l.price_cents, l.listing_url, l.city_name
        FROM worker_listings wl
        LEFT JOIN listings l ON l.item_id = wl.item_id
        WHERE wl.user_id = ? AND date(wl.received_at) = date('now')
        ORDER BY wl.received_at DESC
    """, (user_id,)).fetchall()
    conn.close()
    return [
        {
            "item_id": r[0],
            "received_at": r[1],
            "title": r[2] or "?",
            "price_cents": r[3],
            "listing_url": r[4] or "",
            "city_name": r[5] or "",
        }
        for r in rows
    ]


def get_pending_users(db_path: str) -> list[tuple[int, str]]:
    """(user_id, created_at) для ожидающих подтверждения."""
    conn = get_conn(db_path)
    rows = conn.execute("""
        SELECT user_id, created_at FROM users
        WHERE authorized = 0 AND user_id NOT IN (SELECT user_id FROM blocked_users)
        ORDER BY created_at DESC
    """).fetchall()
    conn.close()
    return [(r[0], r[1] or "") for r in rows]


def get_all_workers(db_path: str) -> list[tuple[int, str, int]]:
    """(user_id, created_at, shift_active) для авторизованных."""
    conn = get_conn(db_path)
    try:
        rows = conn.execute("""
            SELECT user_id, created_at, COALESCE(shift_active, 0)
            FROM users WHERE authorized = 1 AND user_id NOT IN (SELECT user_id FROM blocked_users)
            ORDER BY created_at DESC
        """).fetchall()
    except sqlite3.OperationalError:
        rows = conn.execute("""
            SELECT user_id, created_at, 0 FROM users
            WHERE authorized = 1 AND user_id NOT IN (SELECT user_id FROM blocked_users)
            ORDER BY created_at DESC
        """).fetchall()
    conn.close()
    return [(r[0], r[1] or "", r[2]) for r in rows]


def get_workers_with_stats(db_path: str) -> list[dict]:
    """Воркеры с расширенной статистикой."""
    conn = get_conn(db_path)
    try:
        rows = conn.execute("""
            SELECT u.user_id, u.created_at, COALESCE(u.shift_active, 0)
            FROM users u
            WHERE u.authorized = 1 AND u.user_id NOT IN (SELECT user_id FROM blocked_users)
            ORDER BY u.created_at DESC
        """).fetchall()
        today_counts = {}
        last_times = {}
        try:
            for row in conn.execute("""
                SELECT user_id, COUNT(*) FROM worker_listings
                WHERE date(received_at) = date('now') GROUP BY user_id
            """).fetchall():
                today_counts[row[0]] = row[1]
            for row in conn.execute("""
                SELECT user_id, MAX(received_at) FROM worker_listings GROUP BY user_id
            """).fetchall():
                last_times[row[0]] = (row[1] or "")[:16].replace("T", " ") if row[1] else ""
        except sqlite3.OperationalError:
            pass
    except sqlite3.OperationalError:
        rows = conn.execute("""
            SELECT user_id, created_at, 0 FROM users
            WHERE authorized = 1 AND user_id NOT IN (SELECT user_id FROM blocked_users)
            ORDER BY created_at DESC
        """).fetchall()
        today_counts = {}
        last_times = {}
    conn.close()
    result = []
    for r in rows:
        uid = r[0]
        result.append({
            "user_id": uid,
            "created_at": r[1] or "",
            "shift_active": r[2],
            "listings_today": today_counts.get(uid, 0),
            "last_listing_at": last_times.get(uid, "") or "—",
        })
    return result


def get_blocked_users(db_path: str) -> list[tuple[int, str]]:
    """(user_id, blocked_at) заблокированных."""
    conn = get_conn(db_path)
    rows = conn.execute("SELECT user_id, blocked_at FROM blocked_users ORDER BY blocked_at DESC").fetchall()
    conn.close()
    return [(r[0], r[1] or "") for r in rows]


# --- Почты (email:password) ---
def add_email(db_path: str, email: str, password: str = "") -> bool:
    """Добавить почту. Возвращает True если добавлена, False если уже есть."""
    email = (email or "").strip().lower()
    if not email or "@" not in email:
        return False
    conn = get_conn(db_path)
    try:
        conn.execute(
            "INSERT INTO emails (email, password, created_at) VALUES (?, ?, ?)",
            (email, (password or "").strip(), datetime.utcnow().isoformat()),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        conn.rollback()
        return False
    finally:
        conn.close()


def add_emails_batch(db_path: str, pairs: list[tuple[str, str]]) -> tuple[int, int]:
    """Добавить пачку (email, password). Возвращает (добавлено, пропущено дубликатов)."""
    added, skipped = 0, 0
    conn = get_conn(db_path)
    for email, password in pairs:
        email = (email or "").strip().lower()
        if not email or "@" not in email:
            continue
        try:
            conn.execute(
                "INSERT INTO emails (email, password, created_at) VALUES (?, ?, ?)",
                (email, (password or "").strip(), datetime.utcnow().isoformat()),
            )
            added += 1
        except sqlite3.IntegrityError:
            skipped += 1
    conn.commit()
    conn.close()
    return added, skipped


def get_emails(db_path: str, limit: int = 100, offset: int = 0) -> list[tuple[str, str, str, int]]:
    """(email, password, created_at, blocked) список почт."""
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT email, password, created_at, COALESCE(blocked, 0) FROM emails ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    conn.close()
    return [(r[0], r[1] or "", r[2] or "", r[3] or 0) for r in rows]


def get_emails_count(db_path: str, include_blocked: bool = True) -> int:
    conn = get_conn(db_path)
    if include_blocked:
        row = conn.execute("SELECT COUNT(*) FROM emails").fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) FROM emails WHERE COALESCE(blocked, 0) = 0").fetchone()
    conn.close()
    return row[0] if row else 0


def get_random_email(db_path: str) -> tuple[str, str] | None:
    """Вернуть случайную не заблокированную почту (email, password) или None."""
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT email, password FROM emails WHERE COALESCE(blocked, 0) = 0 ORDER BY RANDOM() LIMIT 1"
    ).fetchone()
    conn.close()
    return (row[0], row[1] or "") if row else None


def get_active_emails(db_path: str) -> list[tuple[str, str]]:
    """Список активных (не заблокированных) почт: [(email, password), ...], по порядку email."""
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT email, password FROM emails WHERE COALESCE(blocked, 0) = 0 ORDER BY email"
    ).fetchall()
    conn.close()
    return [(r[0], r[1] or "") for r in rows]


def get_next_email_for_listing(db_path: str) -> tuple[str, str] | None:
    """
    Round-robin по активным почтам: 1-е объявление — почта 1, 2-е — почта 2, 3-е — почта 3,
    если почт меньше — цикл: 1, 2, 1, 2, 1... Только не заблокированные.
    """
    emails = get_active_emails(db_path)
    if not emails:
        return None
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT value FROM rotation_state WHERE key = 'last_email_for_listing'"
    ).fetchone()
    conn.close()
    last_email = (row[0] or "").strip().lower() if row and row[0] else None
    try:
        idx = next(i for i, (e, _) in enumerate(emails) if (e or "").strip().lower() == last_email)
        next_idx = (idx + 1) % len(emails)
    except StopIteration:
        next_idx = 0
    return emails[next_idx]


def set_last_email_for_listing(db_path: str, email: str) -> None:
    """Записать последнюю почту, использованную для объявления (для round-robin)."""
    conn = get_conn(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO rotation_state (key, value) VALUES ('last_email_for_listing', ?)",
        (email.strip().lower(),),
    )
    conn.commit()
    conn.close()


def mark_email_blocked(db_path: str, email: str) -> bool:
    """Пометить почту как заблокированную. Возвращает True."""
    email = (email or "").strip().lower()
    if not email:
        return False
    conn = get_conn(db_path)
    conn.execute("UPDATE emails SET blocked = 1 WHERE email = ?", (email,))
    conn.commit()
    conn.close()
    return True


def unblock_email(db_path: str, email: str) -> bool:
    """Снять блок с почты."""
    email = (email or "").strip().lower()
    if not email:
        return False
    conn = get_conn(db_path)
    cur = conn.execute("UPDATE emails SET blocked = 0 WHERE email = ?", (email,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def get_last_used_email(db_path: str) -> str | None:
    """Почта, которая последней успешно отправила письмо."""
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT value FROM rotation_state WHERE key = 'last_used_email'"
    ).fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def set_last_used_email(db_path: str, email: str | None) -> None:
    """Записать последнюю использованную почту."""
    conn = get_conn(db_path)
    if email is None:
        conn.execute("DELETE FROM rotation_state WHERE key = 'last_used_email'")
    else:
        conn.execute(
            "INSERT OR REPLACE INTO rotation_state (key, value) VALUES ('last_used_email', ?)",
            (email.strip().lower(),),
        )
    conn.commit()
    conn.close()


def get_all_emails(db_path: str) -> list[tuple[str, str, int]]:
    """Все почты: (email, password, blocked)."""
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT email, password, COALESCE(blocked, 0) FROM emails ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return [(r[0], r[1] or "", r[2] or 0) for r in rows]


def delete_email(db_path: str, email: str) -> bool:
    """Удалить почту. Возвращает True если удалена."""
    email = (email or "").strip().lower()
    conn = get_conn(db_path)
    cur = conn.execute("DELETE FROM emails WHERE email = ?", (email,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


# --- Шаблоны сообщений для писем ---
TEMPLATE_VARS = {
    "url": "Ссылка на объявление",
    "title": "Название товара",
    "price": "Цена (€X.XX)",
    "price_cents": "Цена в центах",
    "seller_name": "Имя продавца",
    "city": "Город",
    "category": "Категория",
    "description": "Описание (первые 500 символов)",
    "user_name": "Имя отправителя (из почты)",
    "item_id": "ID объявления",
}


def add_template(db_path: str, name: str, body: str) -> int:
    """Добавить шаблон. Возвращает id."""
    conn = get_conn(db_path)
    cur = conn.execute(
        "INSERT INTO email_templates (name, body, created_at) VALUES (?, ?, ?)",
        (name.strip(), body.strip(), datetime.utcnow().isoformat()),
    )
    conn.commit()
    rowid = cur.lastrowid
    conn.close()
    return rowid or 0


def get_templates(db_path: str) -> list[tuple[int, str, str, str]]:
    """(id, name, body, created_at) список шаблонов."""
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT id, name, body, created_at FROM email_templates ORDER BY id"
    ).fetchall()
    conn.close()
    return [(r[0], r[1] or "", r[2] or "", r[3] or "") for r in rows]


def get_template(db_path: str, template_id: int) -> tuple[str, str] | None:
    """(name, body) или None."""
    conn = get_conn(db_path)
    row = conn.execute("SELECT name, body FROM email_templates WHERE id = ?", (template_id,)).fetchone()
    conn.close()
    return (row[0], row[1]) if row else None


def update_template(db_path: str, template_id: int, name: str, body: str) -> bool:
    conn = get_conn(db_path)
    cur = conn.execute(
        "UPDATE email_templates SET name = ?, body = ? WHERE id = ?",
        (name.strip(), body.strip(), template_id),
    )
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def delete_template(db_path: str, template_id: int) -> bool:
    conn = get_conn(db_path)
    cur = conn.execute("DELETE FROM email_templates WHERE id = ?", (template_id,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def get_active_template_id(db_path: str) -> int | None:
    """ID активного шаблона или None."""
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT value FROM rotation_state WHERE key = 'active_template_id'"
    ).fetchone()
    conn.close()
    if not row or not row[0]:
        return None
    try:
        return int(row[0])
    except (ValueError, TypeError):
        return None


def set_active_template_id(db_path: str, template_id: int | None) -> None:
    """Установить активный шаблон (None = сбросить)."""
    conn = get_conn(db_path)
    if template_id is None:
        conn.execute("DELETE FROM rotation_state WHERE key = 'active_template_id'")
    else:
        conn.execute(
            "INSERT OR REPLACE INTO rotation_state (key, value) VALUES ('active_template_id', ?)",
            (str(template_id),),
        )
    conn.commit()
    conn.close()


def format_template_example(body: str) -> str:
    """Пример с подставленными переменными."""
    return format_template(body, {
        "url": "https://marktplaats.nl/v/example/m1234567890",
        "title": "iPhone 14 Pro",
        "price": "€899.00",
        "price_cents": "89900",
        "seller_name": "Jan",
        "city": "Amsterdam",
        "category": "Телекоммуникация",
        "description": "Отличное состояние, мало использовался...",
        "user_name": "Мария",
        "item_id": "m1234567890",
    })


def format_template(body: str, vars_dict: dict) -> str:
    """Подставить переменные в шаблон."""
    result = body
    for k, v in vars_dict.items():
        result = result.replace("{" + k + "}", str(v or ""))
    return result


def parse_email_line(line: str) -> tuple[str, str] | None:
    """Парсит строку email:password или email;password. Возвращает (email, password) или None."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    for sep in (":", ";", "\t", " "):
        if sep in line:
            parts = line.split(sep, 1)
            if len(parts) >= 2 and "@" in parts[0]:
                return parts[0].strip().lower(), parts[1].strip()
    if "@" in line:
        return line.lower(), ""
    return None


def parse_emails_text(text: str) -> list[tuple[str, str]]:
    """Парсит текст: каждая строка email:password или email;password."""
    pairs = []
    for line in text.splitlines():
        p = parse_email_line(line)
        if p:
            pairs.append(p)
    return pairs


def parse_emails_csv(csv_content: str) -> list[tuple[str, str]]:
    """Парсит CSV: ищет колонки email/почта/логин и password/пароль."""
    pairs = []
    try:
        reader = csv.reader(io.StringIO(csv_content))
        rows = list(reader)
        if rows and len(rows[0]) == 1 and ";" in (rows[0][0] or ""):
            reader = csv.reader(io.StringIO(csv_content), delimiter=";")
            rows = list(reader)
        if not rows:
            return pairs
        header = [h.lower().strip() for h in rows[0]]
        email_col = None
        pass_col = None
        for i, h in enumerate(header):
            if h in ("email", "почта", "mail", "логин", "login", "username"):
                email_col = i
            if h in ("password", "пароль", "pass", "pwd"):
                pass_col = i
        if email_col is None:
            for i, h in enumerate(header):
                if "@" in str(h):
                    email_col = i
                    break
        if email_col is None:
            return pairs
        if pass_col is None:
            pass_col = email_col + 1 if email_col + 1 < len(header) else email_col
        for row in rows[1:]:
            if len(row) > max(email_col, pass_col):
                email = (row[email_col] or "").strip().lower()
                password = (row[pass_col] or "").strip() if pass_col < len(row) else ""
                if email and "@" in email:
                    pairs.append((email, password))
    except Exception:
        pass
    return pairs


def get_last_update_date(db_path: str) -> str | None:
    if not Path(db_path).exists():
        return None
    conn = get_conn(db_path)
    row = conn.execute("SELECT MAX(crawled_timestamp) FROM listings").fetchone()
    conn.close()
    if not row or not row[0]:
        return None
    ts = row[0]
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y %H:%M")
    except (ValueError, TypeError):
        return ts[:16] if ts else None


# Slug категории из URL -> человеческое название
SLUG_TO_NAME: dict[str, str] = {
    "antiek-en-kunst": "Антиквариат и искусство",
    "audio-tv-en-foto": "Аудио, ТВ и фото",
    "auto-s": "Автомобили",
    "auto-diversen": "Разное для автомобилей",
    "auto-onderdelen": "Автозапчасти",
    "boeken": "Книги",
    "caravans-en-kamperen": "Караваны и кемпинг",
    "cd-s-en-dvd-s": "CD и DVD",
    "computers-en-software": "Компьютеры",
    "contacten-en-berichten": "Контакты и сообщения",
    "dieren-en-toebehoren": "Животные",
    "diensten-en-vakmensen": "Услуги и специалисты",
    "diversen": "Разное",
    "doe-het-zelf-en-verbouw": "Ремонт и стройка",
    "fietsen-en-brommers": "Велосипеды",
    "hobby-en-vrije-tijd": "Хобби",
    "huis-en-inrichting": "Дом и интерьер",
    "huizen-en-kamers": "Дома и комнаты",
    "kinderen-en-baby-s": "Дети и малыши",
    "kleding-dames": "Одежда женская",
    "kleding-heren": "Одежда мужская",
    "motoren": "Мотоциклы",
    "muziek-en-instrumenten": "Музыка",
    "postzegels-en-munten": "Марки и монеты",
    "sieraden-tassen-en-uiterlijk": "Украшения и аксессуары",
    "spelcomputers-en-games": "Игровые приставки и игры",
    "sport-en-fitness": "Спорт",
    "telecommunicatie": "Телекоммуникации",
    "tuin-en-terras": "Сад",
    "vacatures": "Вакансии",
    "vakantie": "Отдых и отпуск",
    "verzamelen": "Коллекционирование",
    "watersport-en-boten": "Водный спорт и лодки",
    "witgoed-en-apparatuur": "Бытовая техника и приборы",
    "witgoed-en-huishoudelijk": "Бытовая техника",
    "zakelijke-goederen": "Бизнес",
}


def _category_slug_from_url(url: str | None) -> str | None:
    if not url or "/v/" not in url:
        return None
    try:
        after = url.split("/v/", 1)[1]
        return after.split("/")[0] if after else None
    except IndexError:
        return None


def get_categories(db_path: str) -> list[tuple[str, str]]:
    """Список категорий (slug, человеческое название). Без ID."""
    if not Path(db_path).exists():
        return []
    conn = get_conn(db_path)
    rows = conn.execute(
        """SELECT DISTINCT listing_url FROM listings
           WHERE listing_url IS NOT NULL AND listing_url != ''"""
    ).fetchall()
    conn.close()
    seen: set[str] = set()
    result: list[tuple[str, str]] = []
    for (url,) in rows:
        slug = _category_slug_from_url(url)
        if slug and slug not in seen:
            seen.add(slug)
            name = SLUG_TO_NAME.get(slug, slug.replace("-", " ").title())
            result.append((slug, name))
    result.sort(key=lambda x: x[1].lower())
    return result


def get_listings(
    db_path: str,
    limit: int = 50,
    min_date: str | None = None,
    category_slug: str | None = None,
    min_price_cents: int | None = None,
    max_price_cents: int | None = None,
    sort_by: str = "listed_timestamp",
    sort_desc: bool = True,
) -> list[dict]:
    """Товары с фильтрами. category_slug — slug из URL, не ID."""
    conn = get_conn(db_path)
    q = "SELECT * FROM listings WHERE 1=1"
    params: list = []
    if min_date:
        q += " AND (listed_timestamp >= ? OR listed_timestamp LIKE ?)"
        params.extend([min_date, f"{min_date}%"])
    if category_slug:
        q += " AND listing_url LIKE ?"
        params.append(f"%/v/{category_slug}/%")
    if min_price_cents is not None:
        q += " AND (price_cents IS NULL OR price_cents >= ?)"
        params.append(min_price_cents)
    if max_price_cents is not None:
        q += " AND (price_cents IS NULL OR price_cents <= ?)"
        params.append(max_price_cents)

    order = "DESC" if sort_desc else "ASC"
    safe_sort = sort_by if sort_by in [f[0] for f in SORT_FIELDS] else "listed_timestamp"
    q += f" ORDER BY {safe_sort} {order} LIMIT ?"
    params.append(limit)

    rows = conn.execute(q, params).fetchall()
    cols = [d[1] for d in conn.execute("PRAGMA table_info(listings)").fetchall()]
    conn.close()
    return [dict(zip(cols, r)) for r in rows]


# Поля для экспорта CSV — только человекочитаемые, без ID
EXPORT_COLUMNS = [
    ("title", "Название"),
    ("description", "Описание"),
    ("price_cents", "Цена (€)"),
    ("price_type", "Тип цены"),
    ("ad_type", "Тип объявления"),
    ("city_name", "Город"),
    ("country_code", "Страна"),
    ("country_name", "Страна (полное)"),
    ("seller_name", "Продавец"),
    ("listed_timestamp", "Дата публикации"),
    ("crawled_timestamp", "Дата сбора"),
    ("view_count", "Просмотры"),
    ("favorited_count", "В избранном"),
    ("types", "Типы"),
    ("services", "Услуги"),
    ("category_verticals", "Категории"),
    ("priority_product", "Приоритет"),
    ("traits", "Признаки"),
    ("distance_meters", "Расстояние (м)"),
    ("listing_url", "Ссылка"),
    ("image_urls", "Изображения"),
    ("category_specific_description", "Описание категории"),
    ("reserved", "Зарезервировано"),
    ("nap_available", "NAP доступен"),
    ("urgency_feature_active", "Срочность"),
    ("is_verified", "Верифицирован"),
    ("seller_website_url", "Сайт продавца"),
    ("attributes_json", "Атрибуты"),
]


def export_listings_to_csv(items: list[dict]) -> bytes:
    """Экспорт с человекочитаемыми заголовками, без ID."""
    if not items:
        return b""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([ru for _, ru in EXPORT_COLUMNS])
    for it in items:
        row = []
        for en, _ in EXPORT_COLUMNS:
            val = it.get(en)
            if val is None:
                val = ""
            elif isinstance(val, (int, float)) and en == "price_cents":
                val = f"{val / 100:.2f}"
            row.append(val)
        writer.writerow(row)
    return buf.getvalue().encode("utf-8-sig")
