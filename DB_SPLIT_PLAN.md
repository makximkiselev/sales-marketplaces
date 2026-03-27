# DB Split Plan

## Target Layout

### 1. System DB
Configuration, dictionaries, integrations, store-level settings.

Tables:
- `stores`
- `store_settings`
- `store_datasets`
- `source_tables_registry`
- `integrations`
- `refresh_jobs`
- `pricing_category_tree`
- `pricing_category_settings`
- `pricing_logistics_product_settings`
- `fx_rates_cache`
- `category_tree_cache_nodes`

### 2. Operational DB
Current state used by UI, strategy, export, monitoring.

Tables:
- `pricing_price_results`
- `pricing_boost_results`
- `pricing_strategy_results`
- `pricing_attractiveness_results`
- `pricing_promo_results`
- `pricing_promo_offer_results`
- `sales_overview_order_rows`
- `sales_overview_cogs_source_rows`
- `yandex_goods_price_report_items`
- `refresh_job_runs`
- `pricing_autopilot_decisions`
- dynamic `source_items__*`
- `db_explorer_catalog`

### 3. History DB
Raw reports, snapshots, export history, long retrospective records.

Tables:
- `pricing_strategy_history`
- `pricing_strategy_iteration_history`
- `pricing_market_price_export_history`
- `pricing_cogs_snapshots`
- `pricing_autopilot_snapshots`
- `sales_market_order_items`
- `sales_united_order_transactions`
- `sales_united_netting_report_rows`
- `sales_shelfs_statistics_report_rows`
- `sales_shows_boost_report_rows`
- `yandex_goods_price_report_history`
- `pricing_promo_campaign_raw`
- `pricing_promo_offer_raw`

## Settings Consolidation

`store_settings` is the main consolidated system table.

Columns:
- `store_uid`
- `pricing_json`
- `logistics_json`
- `sources_json`
- `export_json`
- `sales_plan_json`
- `updated_at`

Compatibility phase:
- existing tables remain readable/writable
- writes are mirrored into `store_settings`
- reads will be moved page by page

## Current Migration Order

1. Introduce `system DB` and `store_settings`
2. Mirror writes from current settings tables into `store_settings`
3. Move settings page reads to `store_settings`
4. Keep category/product-specific tables separate
5. Move operational latest-state tables to operational Postgres
6. Move raw history to history Postgres
7. Add retention/aggregation rules for heavy history tables

Migration tool:
- `python3 tools/migrate_split_postgres.py`
- DSN envs:
  - `APP_SYSTEM_DATABASE_URL`
  - `APP_DATABASE_URL`
  - `APP_HISTORY_DATABASE_URL`

## Heavy Tables To Reduce First

- `pricing_cogs_snapshots`
- `pricing_market_price_export_history`
- `pricing_autopilot_snapshots`
- `pricing_strategy_iteration_history`
- `sales_united_netting_report_rows`

## Migration Matrix

| Current table | Target DB | Target table | Action |
|---|---|---|---|
| `stores` | system | `stores` | keep |
| `pricing_store_settings` | system | `store_settings` | merge into json |
| `pricing_logistics_store_settings` | system | `store_settings` | merge into json |
| `refresh_jobs` | system | `refresh_jobs` | keep |
| `source_tables_registry` | system | `source_tables_registry` | keep |
| `pricing_category_tree` | system | `pricing_category_tree` | keep |
| `pricing_category_settings` | system | `pricing_category_settings` | keep |
| `pricing_logistics_product_settings` | system | `pricing_logistics_product_settings` | keep |
| `pricing_price_results` | operational | `pricing_price_results` | keep |
| `pricing_boost_results` | operational | `pricing_boost_results` | keep |
| `pricing_strategy_results` | operational | `pricing_strategy_results` | keep |
| `pricing_attractiveness_results` | operational | `pricing_attractiveness_results` | keep |
| `pricing_promo_results` | operational | `pricing_promo_results` | keep |
| `pricing_promo_offer_results` | operational | `pricing_promo_offer_results` | keep |
| `sales_overview_order_rows` | operational | `sales_overview_order_rows` | keep |
| `sales_overview_cogs_source_rows` | operational | `sales_overview_cogs_source_rows` | keep |
| `refresh_job_runs` | operational | `refresh_job_runs` | keep |
| `source_items__*` | operational | `source_items__*` | keep |
| `pricing_strategy_history` | history | `pricing_strategy_history` | move |
| `pricing_strategy_iteration_history` | history | `pricing_strategy_iteration_history` | move + retention |
| `pricing_market_price_export_history` | history | `pricing_market_price_export_history` | move + retention |
| `pricing_cogs_snapshots` | history | `pricing_cogs_snapshots` | move + aggressive cleanup |
| `pricing_autopilot_snapshots` | history | `pricing_autopilot_snapshots` | move + retention |
| `sales_united_netting_report_rows` | history | `sales_united_netting_report_rows` | move + retention |
| `sales_united_order_transactions` | history | `sales_united_order_transactions` | move |
| `sales_market_order_items` | history | `sales_market_order_items` | move |
