# CLAUDE.md

Guidance for Claude Code when working in this repo.

## What this is

A small ETL that pulls real estate listings from **Realtor.com** (via the `realty-in-us` RapidAPI wrapper) and stores them in a local SQLite database for ad-hoc analysis. Current sample covers Belleville, NJ and 7 neighboring towns (Nutley, Bloomfield, Kearny, Lyndhurst, North Arlington, Clifton, Harrison) — both for-sale and for-rent, ~1000-1500 unique listings.

History: the project originally targeted the SimplyRETS demo feed (fake Texas data). It now uses Realtor.com because SimplyRETS only serves realistic data for licensed MLS subscribers.

## Layout

- `fetch.py` — POSTs to `/properties/v3/list`, flattens each `SearchHome` result, and replaces the `properties` table with a fresh sample. Single file, raw SQL — no ORM.
- `test_fetch.py` — pytest unit tests for `flatten()`.
- `pyproject.toml` / `uv.lock` — dependencies managed by [uv](https://docs.astral.sh/uv/); runtime dep `requests`, dev dep `pytest` (`sqlite3` is stdlib).
- `properties.db` — SQLite file (gitignored).
- `.env` (gitignored) — optional local file for `RAPIDAPI_KEY`.

## Config

`fetch.py` is driven by two top-level constants:
- `CITIES` — list of `(city, state_code)` tuples.
- `STATUSES` — list of status filters; `fetch.py` runs every status against every city, so each city is always queried for both sale and rent.

Current setup: Belleville + 7 neighboring NJ towns, both `["for_sale", "ready_to_build"]` and `["for_rent"]`. Each run uses ~16 API calls (2 per city).

## Schema

Single `properties` table, PK `property_id` (Realtor.com's stable property identifier). Notable columns:

- Status/pricing: `status` (`for_sale`, `for_rent`, `sold`, etc.), `list_price` (monthly rent when `status='for_rent'`), `list_date`, `last_update`
- Property: `property_type`, `sub_type`, `bedrooms`, `baths_full`, `baths_half`, `baths_total`, `area_sqft`, `lot_sqft`, `year_built`, `stories`
- Location: `address_line`, `city`, `state`, `postal_code`, `latitude`, `longitude`, `county_fips`
- Costs: `hoa_fee`
- People/media: `agent_name`, `office_name`, `primary_photo`
- Freeform: `tags_json` (JSON array), `extra_info` (JSON blob of flags/open_houses/virtual_tours/matterport/photo_count)
- Bookkeeping: `fetched_at`

Indexes on `city`, `status`, `list_price`.

`fetch.py` **drops and recreates** the `properties` table on every run — no upsert-over-history, no migration story. If you add columns to `SCHEMA`/`UPSERT`/`flatten()`, just rerun.

## Run

```bash
uv sync                                  # install deps (incl. dev) into .venv
export RAPIDAPI_KEY="your-key-here"      # required
uv run fetch.py                          # drops + refills properties.db
uv run pytest -q                         # run tests
```

## API notes

- Endpoint: `POST https://realty-in-us.p.rapidapi.com/properties/v3/list`
- Auth: `X-RapidAPI-Key` header, subscribed via RapidAPI
- Max page size tested: 200+. Pagination via `offset`; `data.home_search.total` gives the full count.
- Rentals: pass `status: ["for_rent"]` on the same endpoint. The separate `/list-for-rent` paths 404 or 204.
- Free tier: ~500 calls/month. Current 8-city run uses ~16 calls.
