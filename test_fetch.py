"""Tests for fetch.flatten — verifies Realtor.com SearchHome → row mapping."""

import json

from fetch import flatten


SAMPLE = {
    "property_id": "5578691819",
    "listing_id": "2994102572",
    "status": "for_sale",
    "list_price": 525000,
    "list_date": "2026-04-01T12:00:00Z",
    "last_update_date": "2026-04-10T08:00:00Z",
    "photo_count": 36,
    "matterport": False,
    "virtual_tours": None,
    "open_houses": None,
    "flags": {"is_new_listing": False, "is_contingent": None},
    "tags": ["central_air", "garage"],
    "branding": [{"name": "NJ METRO GROUP"}],
    "advertisers": [{"name": "Jessica Keefe, Agent"}],
    "primary_photo": {"href": "https://example.com/photo.jpg"},
    "hoa": {"fee": 150},
    "location": {
        "address": {
            "line": "31-33 Smallwood Ave",
            "city": "Belleville",
            "state_code": "NJ",
            "state": "New Jersey",
            "postal_code": "07109",
            "coordinate": {"lat": 40.79, "lon": -74.17},
        },
        "county": {"fips_code": "34013"},
    },
    "description": {
        "type": "single_family",
        "sub_type": None,
        "beds": 3,
        "baths": 3,
        "baths_full": 2,
        "baths_half": 1,
        "sqft": 1850,
        "lot_sqft": 4000,
        "year_built": 1935,
        "stories": 2,
    },
}


def test_flatten_core_fields():
    row = flatten(SAMPLE)
    assert row["property_id"] == "5578691819"
    assert row["status"] == "for_sale"
    assert row["list_price"] == 525000
    assert row["city"] == "Belleville"
    assert row["state"] == "NJ"
    assert row["postal_code"] == "07109"
    assert row["latitude"] == 40.79
    assert row["longitude"] == -74.17
    assert row["bedrooms"] == 3
    assert row["baths_full"] == 2
    assert row["baths_half"] == 1
    assert row["area_sqft"] == 1850
    assert row["year_built"] == 1935
    assert row["agent_name"] == "Jessica Keefe, Agent"
    assert row["office_name"] == "NJ METRO GROUP"
    assert row["hoa_fee"] == 150
    assert row["county_fips"] == "34013"
    assert row["primary_photo"] == "https://example.com/photo.jpg"
    assert row["fetched_at"]  # non-empty ISO timestamp


def test_flatten_tags_and_extra_info_are_json():
    row = flatten(SAMPLE)
    assert json.loads(row["tags_json"]) == ["central_air", "garage"]
    extra = json.loads(row["extra_info"])
    assert extra["photo_count"] == 36
    assert extra["flags"] == {"is_new_listing": False, "is_contingent": None}


def test_flatten_handles_missing_nested_objects():
    minimal = {"property_id": "x", "status": "for_rent", "list_price": 2100}
    row = flatten(minimal)
    assert row["property_id"] == "x"
    assert row["list_price"] == 2100
    assert row["city"] is None
    assert row["bedrooms"] is None
    assert row["agent_name"] is None
    assert row["office_name"] is None
    assert json.loads(row["tags_json"]) == []
