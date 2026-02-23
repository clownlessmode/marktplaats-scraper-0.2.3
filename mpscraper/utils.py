import pandas as pd
from datetime import datetime, timezone
from pathlib import Path

from .exceptions import EmptyDataFrameError

# Русские названия столбцов для CSV
COLUMNS_RU = {
    "item_id": "id_объявления",
    "seller_id": "id_продавца",
    "parent_category_id": "id_родительской_категории",
    "child_category_id": "id_подкатегории",
    "category_verticals": "категории",
    "ad_type": "тип_объявления",
    "title": "название",
    "description": "описание",
    "price_type": "тип_цены",
    "price_cents": "цена_центы",
    "types": "типы",
    "services": "услуги",
    "listing_url": "ссылка",
    "image_urls": "изображения",
    "city_name": "город",
    "country_code": "страна",
    "listed_timestamp": "дата_публикации",
    "crawled_timestamp": "дата_сбора",
    "view_count": "просмотры",
    "favorited_count": "в_избранном",
    # Расширенные поля из __NEXT_DATA__
    "seller_name": "имя_продавца",
    "latitude": "широта",
    "longitude": "долгота",
    "distance_meters": "расстояние_м",
    "country_name": "страна_название",
    "priority_product": "приоритет_товара",
    "traits": "признаки",
    "category_specific_description": "описание_категории",
    "reserved": "зарезервировано",
    "nap_available": "nap_доступен",
    "urgency_feature_active": "срочность",
    "is_verified": "продавец_верифицирован",
    "seller_website_url": "сайт_продавца",
    "attributes_json": "атрибуты_json",
}
COLUMNS_EN = {v: k for k, v in COLUMNS_RU.items()}


def read_csv(file_path: str) -> pd.DataFrame:
    """Read CSV file into DataFrame from the given path."""
    try:
        df = pd.read_csv(file_path)
        # Если столбцы на русском — переводим обратно в английские
        if df.columns.any() and df.columns[0] in COLUMNS_EN:
            df = df.rename(columns=COLUMNS_EN)
        return df
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def get_utc_now() -> datetime:
    """Return the current time in UTC timezone"""
    return datetime.now(tz=timezone.utc)


def get_utc_iso_now() -> str:
    """Return the ISO 8601 UTC timestamp string for now()."""
    return get_utc_now().isoformat()


def diff_hours(first: datetime, last: datetime) -> float:
    """Return the difference in hours between first-last datetimes."""
    diff = last - first
    days, seconds = diff.days, diff.seconds

    return (days * 24) + (seconds / 3600)


def handle_sigterm_interrupt(*args):
    """Raise KeyboardInterrupt for the given signal."""
    raise KeyboardInterrupt()


def remove_duplicate_listings(df: pd.DataFrame) -> pd.DataFrame:
    """Return the DataFrame with duplicate listings removed."""
    df_no_dupes = df.drop_duplicates(
        subset=["item_id"], keep="last", ignore_index=True, inplace=False
    )

    if df_no_dupes is not None:
        return df_no_dupes
    else:
        raise EmptyDataFrameError()


def save_listings(listings_df: pd.DataFrame, file_path: str):
    """Save the DataFrame of listings to the given file path."""
    path = Path(file_path)

    # make the parent directories if they do not exist
    dir_path = path.parent
    dir_path.mkdir(parents=True, exist_ok=True)

    # Сохраняем с русскими названиями столбцов
    rename_map = {k: v for k, v in COLUMNS_RU.items() if k in listings_df.columns}
    df_ru = listings_df.rename(columns=rename_map)
    df_ru.to_csv(file_path, index=False)


def format_text(text: str) -> str:
    """Return the given text with excess whitespace trimmed to singular space."""

    def remove_multi_whitespace(text: str) -> str:
        return " ".join(text.split())

    fmt = remove_multi_whitespace(text)

    return fmt
