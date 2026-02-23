#!/usr/bin/env python3
"""
Тест полного пути: новое объявление → воркеру в Telegram → письмо продавцу.
Использует те же функции, что и fetch_listings при находке нового объявления.

Запуск: python test_listing_flow.py
       python test_listing_flow.py --db-path data/bot.db
"""
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timezone

# Загружаем .env
_path = Path(__file__).resolve().parent / ".env"
if _path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_path, override=True)
    except ImportError:
        import os
        for line in _path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v

import argparse
from telegram_bot.database import init_db, get_conn
from telegram_bot.telegram_sender import send_listing_to_next_worker
from telegram_bot.email_sender import try_send_listing_email
from telegram_bot.config import DB_PATH, ENVIRONMENT


def make_test_listing_row() -> dict:
    """Тестовое объявление — те же поля, что details_to_db_row."""
    return {
        "item_id": f"test_flow_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "seller_id": "test_seller_123",
        "parent_category_id": 1,
        "child_category_id": 1,
        "category_verticals": "Телекоммуникация",
        "ad_type": "offer",
        "title": "iPhone 14 Pro — тестовое объявление",
        "description": "Отличное состояние. Тест полного пути: воркер + email.",
        "price_type": "fixed",
        "price_cents": 89900,
        "types": None,
        "services": None,
        "listing_url": "https://www.marktplaats.nl/l/telecommunicatie/mobiele-telefoons-apple-iphone/m1234567890",
        "image_urls": None,
        "city_name": "Amsterdam",
        "country_code": "NL",
        "listed_timestamp": datetime.now(timezone.utc).isoformat(),
        "crawled_timestamp": datetime.now(timezone.utc).isoformat(),
        "view_count": 10,
        "favorited_count": 2,
        "seller_name": "JanDeVries",
        "latitude": None,
        "longitude": None,
        "distance_meters": None,
        "country_name": "Nederland",
        "priority_product": None,
        "traits": None,
        "category_specific_description": None,
        "reserved": 0,
        "nap_available": 0,
        "urgency_feature_active": 0,
        "is_verified": 0,
        "seller_website_url": None,
        "attributes_json": None,
    }


def run_test(db_path: str) -> None:
    row = make_test_listing_row()
    ns = SimpleNamespace(**row)
    title = row["title"][:40]

    print("=" * 50)
    print("ТЕСТ ПОЛНОГО ПУТИ: новое объявление")
    print("=" * 50)
    print(f"item_id: {row['item_id']}")
    print(f"title: {row['title']}")
    print(f"seller_name: {row['seller_name']} → email: {row['seller_name'].lower()}@gmail.com")
    print(f"ENVIRONMENT: {ENVIRONMENT}")
    if ENVIRONMENT == "dev":
        print("→ Письмо уйдёт на eclipselucky@gmail.com (dev)")
    print()

    # 1. Сохраняем в БД (как при находке)
    init_db(db_path)
    from fetch_listings import save_listing_to_db
    save_listing_to_db(row, db_path)
    print("1. ✓ Сохранено в БД")

    # category_ru — для ns, не в БД
    row["category_ru"] = "Телекоммуникация"

    # 2. Отправка воркеру (round-robin)
    ok_tg, worker_id = send_listing_to_next_worker(ns, db_path)
    print(f"2. {'✓' if ok_tg else '✗'} Telegram воркеру: {'OK' if ok_tg else 'пропуск (нет воркеров на смене)'} «{title}»")

    # 3. Письмо продавцу (почты и шаблон воркера, получившего объявление)
    try:
        email_ok, recipient = try_send_listing_email(db_path, ns, worker_id)
        if email_ok and recipient:
            print(f"3. ✓ Email продавцу: отправлено на {recipient} «{title}»")
        else:
            print(f"3. ✗ Email продавцу: пропуск (нет шаблона/почт) «{title}»")
    except Exception as ex:
        print(f"3. ✗ Email: {ex}")

    print()
    print("=" * 50)
    print("Готово. Проверь Telegram и почту (eclipselucky@gmail.com в dev).")
    print("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Тест полного пути: объявление → воркер + email")
    parser.add_argument("--db-path", default=DB_PATH, help="Путь к bot.db")
    args = parser.parse_args()
    run_test(args.db_path)


if __name__ == "__main__":
    main()
