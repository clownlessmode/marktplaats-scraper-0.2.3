"""Конфиг бота."""
import os
from pathlib import Path

# Загружаем .env из корня проекта
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_path)
    except ImportError:
        # Fallback: парсим .env вручную
        for line in _env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUTH_CODE = os.getenv("BOT_AUTH_CODE", "1111")
# Токены: клиентский бот (воркеры) и админский бот
CLIENT_BOT_TOKEN = os.getenv("CLIENT_BOT_TOKEN", os.getenv("BOT_TOKEN", "")).strip()
ADMIN_BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN", "").strip()
BOT_TOKEN = CLIENT_BOT_TOKEN  # обратная совместимость
DATA_DIR = os.path.join(PROJECT_ROOT, os.getenv("MP_DATA_DIR", "."))
LISTINGS_CSV = os.path.join(DATA_DIR, "listings.csv")
# Fallback: listings.csv в корне проекта
if not os.path.exists(LISTINGS_CSV):
    _root_csv = os.path.join(PROJECT_ROOT, "listings.csv")
    if os.path.exists(_root_csv):
        LISTINGS_CSV = _root_csv
DB_PATH = os.path.join(DATA_DIR, "bot.db")
SCRAPER_DIR = PROJECT_ROOT
# Chat ID администратора (обязательно для админ-бота)
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "691976114").strip()
# Chat ID для рассылки (fallback, если нет воркеров на смене)
TELEGRAM_CHAT_ID = os.getenv("MP_TELEGRAM_CHAT_ID", ADMIN_CHAT_ID).strip()
# prod = headless, dev = с окном браузера (для отладки)
ENVIRONMENT = os.getenv("ENVIRONMENT", "prod").lower()
HEADLESS = ENVIRONMENT == "prod"
# Куда слать тестовые письма при проверке почт
TEST_MAIL = os.getenv("TEST_MAIL", "eclipselucky@gmail.com").strip()
