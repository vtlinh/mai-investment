"""Fetch Realtor.com listings (via RapidAPI realty-in-us) and store in SQLite.

Queries a curated list of (city, state, status) tuples, drops the existing
`properties` table, and reinserts a fresh sample. Set RAPIDAPI_KEY in the
environment before running.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests

API_URL = "https://realty-in-us.p.rapidapi.com/properties/v3/list"
API_HOST = "realty-in-us.p.rapidapi.com"
DB_PATH = Path(__file__).parent / "properties.db"
PAGE_SIZE = 200

# Cities to query; every city is fetched for both sale and rent.
CITIES = [
    ("Belleville",      "NJ"),
    ("Nutley",          "NJ"),
    ("Bloomfield",      "NJ"),
    ("Kearny",          "NJ"),
    ("Lyndhurst",       "NJ"),
    ("North Arlington", "NJ"),
    ("Clifton",         "NJ"),
    ("Harrison",        "NJ"),
]
STATUSES = [
    ["for_sale", "ready_to_build"],
    ["for_rent"],
]
MAX_PER_QUERY = 200

SCHEMA = """
DROP TABLE IF EXISTS properties;
CREATE TABLE properties (
    property_id     TEXT PRIMARY KEY,
    listing_id      TEXT,
    status          TEXT,
    list_price      INTEGER,
    list_date       TEXT,
    last_update     TEXT,
    property_type   TEXT,
    sub_type        TEXT,
    bedrooms        INTEGER,
    baths_full      INTEGER,
    baths_half      INTEGER,
    baths_total     REAL,
    area_sqft       INTEGER,
    lot_sqft        INTEGER,
    year_built      INTEGER,
    stories         INTEGER,
    address_line    TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     TEXT,
    latitude        REAL,
    longitude       REAL,
    county_fips     TEXT,
    hoa_fee         INTEGER,
    agent_name      TEXT,
    office_name     TEXT,
    primary_photo   TEXT,
    tags_json       TEXT,
    extra_info      TEXT,
    fetched_at      TEXT NOT NULL
);
CREATE INDEX idx_properties_city   ON properties(city);
CREATE INDEX idx_properties_status ON properties(status);
CREATE INDEX idx_properties_price  ON properties(list_price);
"""

UPSERT = """
INSERT INTO properties (
    property_id, listing_id, status, list_price, list_date, last_update,
    property_type, sub_type, bedrooms, baths_full, baths_half, baths_total,
    area_sqft, lot_sqft, year_built, stories, address_line, city, state,
    postal_code, latitude, longitude, county_fips, hoa_fee, agent_name,
    office_name, primary_photo, tags_json, extra_info, fetched_at
) VALUES (
    :property_id, :listing_id, :status, :list_price, :list_date, :last_update,
    :property_type, :sub_type, :bedrooms, :baths_full, :baths_half, :baths_total,
    :area_sqft, :lot_sqft, :year_built, :stories, :address_line, :city, :state,
    :postal_code, :latitude, :longitude, :county_fips, :hoa_fee, :agent_name,
    :office_name, :primary_photo, :tags_json, :extra_info, :fetched_at
)
ON CONFLICT(property_id) DO UPDATE SET
    status        = excluded.status,
    list_price    = excluded.list_price,
    last_update   = excluded.last_update,
    fetched_at    = excluded.fetched_at
;
"""


def flatten(home):
    loc = home.get("location") or {}
    addr = loc.get("address") or {}
    coord = addr.get("coordinate") or {}
    county = loc.get("county") or {}
    desc = home.get("description") or {}
    hoa = home.get("hoa") or {}
    photo = home.get("primary_photo") or {}
    advertisers = home.get("advertisers") or []
    branding = home.get("branding") or []

    agent_name = advertisers[0].get("name") if advertisers else None
    office_name = branding[0].get("name") if branding else None

    return {
        "property_id":   home.get("property_id"),
        "listing_id":    home.get("listing_id"),
        "status":        home.get("status"),
        "list_price":    home.get("list_price"),
        "list_date":     home.get("list_date"),
        "last_update":   home.get("last_update_date"),
        "property_type": desc.get("type"),
        "sub_type":      desc.get("sub_type"),
        "bedrooms":      desc.get("beds"),
        "baths_full":    desc.get("baths_full"),
        "baths_half":    desc.get("baths_half"),
        "baths_total":   desc.get("baths"),
        "area_sqft":     desc.get("sqft"),
        "lot_sqft":      desc.get("lot_sqft"),
        "year_built":    desc.get("year_built"),
        "stories":       desc.get("stories"),
        "address_line":  addr.get("line"),
        "city":          addr.get("city"),
        "state":         addr.get("state_code"),
        "postal_code":   addr.get("postal_code"),
        "latitude":      coord.get("lat"),
        "longitude":     coord.get("lon"),
        "county_fips":   county.get("fips_code"),
        "hoa_fee":       hoa.get("fee"),
        "agent_name":    agent_name,
        "office_name":   office_name,
        "primary_photo": photo.get("href"),
        "tags_json":     json.dumps(home.get("tags") or []),
        "extra_info":    json.dumps({
            "flags":         home.get("flags"),
            "open_houses":   home.get("open_houses"),
            "virtual_tours": home.get("virtual_tours"),
            "matterport":    home.get("matterport"),
            "photo_count":   home.get("photo_count"),
        }),
        "fetched_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def fetch_page(api_key, city, state_code, status, limit, offset):
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": API_HOST,
        "Content-Type": "application/json",
    }
    payload = {
        "limit": limit,
        "offset": offset,
        "city": city,
        "state_code": state_code,
        "status": status,
        "sort": {"direction": "desc", "field": "list_date"},
    }
    r = requests.post(API_URL, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json().get("data") or {}
    hs = data.get("home_search") or {}
    return hs.get("results") or [], hs.get("total") or 0


def fetch_query(api_key, city, state_code, status, max_rows):
    collected, offset = [], 0
    while len(collected) < max_rows:
        remaining = max_rows - len(collected)
        page, total = fetch_page(api_key, city, state_code, status,
                                 min(PAGE_SIZE, remaining), offset)
        if not page:
            break
        collected.extend(page)
        offset += len(page)
        if offset >= total:
            break
    return collected


def main():
    api_key = os.environ.get("RAPIDAPI_KEY")
    if not api_key:
        raise SystemExit("RAPIDAPI_KEY environment variable is required")

    con = sqlite3.connect(DB_PATH)
    con.executescript(SCHEMA)
    total_inserted = 0
    with con:
        for city, state in CITIES:
            for status in STATUSES:
                homes = fetch_query(api_key, city, state, status, MAX_PER_QUERY)
                for home in homes:
                    row = flatten(home)
                    if not row["property_id"]:
                        continue
                    con.execute(UPSERT, row)
                    total_inserted += 1
                print(f"  {city}, {state} {status}: {len(homes)} rows")
    con.close()
    print(f"Inserted {total_inserted} listings into {DB_PATH}")


if __name__ == "__main__":
    main()
