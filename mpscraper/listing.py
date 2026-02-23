from dataclasses import dataclass


@dataclass
class ListingDetails:
    """Marktplaats Listing Details, scraped from the listing description."""

    description: str
    ad_type: str
    types: set[str]
    services: set[str]
    price_type: str
    price_cents: int
    view_count: int
    favorited_count: int
    listed_timestamp: str


@dataclass
class Listing:
    """Marktplaats Listing."""

    item_id: str
    seller_id: str
    parent_category_id: int
    child_category_id: int
    category_verticals: tuple[str, ...]
    ad_type: str
    title: str
    description: str
    price_type: str
    price_cents: int
    types: tuple[str, ...]
    services: tuple[str, ...]
    listing_url: str
    image_urls: tuple[str, ...]
    city_name: str
    country_code: str
    listed_timestamp: str
    crawled_timestamp: str
    view_count: int
    favorited_count: int
    # Расширенные поля из __NEXT_DATA__ (fast mode)
    seller_name: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    distance_meters: int = -1
    country_name: str = ""
    priority_product: str = ""
    traits: tuple[str, ...] = ()
    category_specific_description: str = ""
    reserved: bool = False
    nap_available: bool = False
    urgency_feature_active: bool = False
    is_verified: bool = False
    seller_website_url: str = ""
    attributes_json: str = ""
