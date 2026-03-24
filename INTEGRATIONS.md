# Maillard Coffee AGI -- Integration Checklist (V1)

## Current Status: Manual Data Mode

All systems work with local JSON files (`data/current_state.json`, `data/recipes.json`, `data/costs.json`, `data/prices.json`).
Live API connectors are built but require credentials to activate.

---

## Square POS

**File:** `scripts/square_connector.py`
**Function:** `get_square_sales()`
**Status:** Built, needs credentials

### Setup
- [ ] Create Square Developer account
- [ ] Generate production access token
- [ ] Set `SQUARE_ACCESS_TOKEN` environment variable
- [ ] Set `SQUARE_LOCATION_ID` environment variable (optional)
- [ ] Test with `python scripts/square_connector.py --dry-run`
- [ ] Run `python scripts/square_connector.py --save` to merge into state

### Fallback
CSV export: `python scripts/ingest_pos.py data/pos_export.csv`

---

## Shopify

**File:** `scripts/shopify_connector.py`
**Function:** `get_shopify_sales()`
**Status:** Built, needs credentials

### Setup
- [ ] Create Shopify private app or custom app
- [ ] Grant `read_orders` scope
- [ ] Set `SHOPIFY_STORE` environment variable (e.g., maillard.myshopify.com)
- [ ] Set `SHOPIFY_ACCESS_TOKEN` environment variable
- [ ] Test with `python scripts/shopify_connector.py --dry-run`
- [ ] Run `python scripts/shopify_connector.py --save` to merge into state

### Fallback
CSV export: `python scripts/ingest_shopify.py data/shopify_export.csv`

---

## QuickBooks

**File:** `scripts/ingest_quickbooks.py`
**Status:** CSV-only (no live API yet)

### Current
- Export invoices as CSV from QuickBooks
- Run `python scripts/ingest_quickbooks.py data/qb_export.csv`

### Future API Setup
- [ ] Register QuickBooks app at developer.intuit.com
- [ ] Implement OAuth2 flow (QuickBooks requires it)
- [ ] Build `scripts/quickbooks_connector.py` with `get_quickbooks_sales()`
- [ ] Endpoint: `GET /v3/company/{id}/query?query=SELECT * FROM Invoice WHERE TxnDate = 'today'`
- [ ] Extract line items: `Line[].SalesItemLineDetail.ItemRef.name` + `Line[].SalesItemLineDetail.Qty`

---

## Inventory Source

**File:** `maillard/mcp/operations/state_loader.py`
**Status:** JSON file (`data/current_state.json`)

### Current
- Manual update: edit `data/current_state.json` -> `inventory` section
- Or use inventory CRUD via API: `POST /api/ops/inventory`

### Future Integration Options
- [ ] Connect to POS inventory counts (Square Inventory API)
- [ ] Connect to accounting system (QuickBooks Items)
- [ ] Barcode scanner input (USB HID -> webhook)
- [ ] Daily count sheet (CSV import)

---

## Daily Sync (All Sources)

**File:** `scripts/sync_data.py`

```bash
# Set credentials, then:
python scripts/sync_data.py
```

Fetches from all configured sources, merges, saves to `current_state.json`.
Skips unavailable sources gracefully.

---

## Architecture (V1 Frozen)

```
data/current_state.json    <-- single source of truth
  |
  +-- Manual edit OR
  +-- scripts/ingest_pos.py (CSV)
  +-- scripts/ingest_shopify.py (CSV)
  +-- scripts/ingest_quickbooks.py (CSV)
  +-- scripts/square_connector.py (API)
  +-- scripts/shopify_connector.py (API)
  +-- scripts/sync_data.py (all APIs)
  |
  v
state_loader.py -> recipes.json -> procurement.py -> decision_engine.py
  |
  v
Morning Brief / Daily Plan / Dashboard
```

No new departments or features until V2.
