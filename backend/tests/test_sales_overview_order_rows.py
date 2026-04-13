import unittest
from unittest.mock import AsyncMock, patch

from backend.services import yandex_united_orders_report_service as svc


class SalesOverviewOrderRowsTests(unittest.IsolatedAsyncioTestCase):
    def test_split_materialized_rows_by_lifecycle(self) -> None:
        history_rows, hot_rows = svc._split_materialized_rows_by_lifecycle(
            [
                {"order_id": "1", "sku": "A", "item_status": "Оформлен"},
                {"order_id": "2", "sku": "B", "item_status": "Отгружен"},
                {"order_id": "3", "sku": "C", "item_status": "Доставлен покупателю"},
                {"order_id": "4", "sku": "D", "item_status": "Возврат"},
                {"order_id": "5", "sku": "E", "item_status": "Отменен"},
            ]
        )

        self.assertEqual([row["order_id"] for row in hot_rows], ["1", "2"])
        self.assertEqual([row["order_id"] for row in history_rows], ["3", "4"])

    def test_split_materialized_rows_clears_delivery_date_for_hot_rows(self) -> None:
        history_rows, hot_rows = svc._split_materialized_rows_by_lifecycle(
            [
                {"order_id": "1", "sku": "A", "item_status": "Оформлен", "delivery_date": "2026-04-07"},
                {"order_id": "2", "sku": "B", "item_status": "Доставлен покупателю", "delivery_date": "2026-04-07"},
            ]
        )

        self.assertEqual(hot_rows[0]["delivery_date"], "")
        self.assertEqual(history_rows[0]["delivery_date"], "2026-04-07")

    async def test_convert_store_amount_to_rub_uses_fx_snapshot(self) -> None:
        with patch.object(svc, "_get_cbr_usd_rub_rate_for_date", AsyncMock(return_value=96.5)) as rate_mock:
            converted = await svc._convert_store_amount_to_rub(
                10.0,
                currency_code="USD",
                fx_rate=None,
                calc_date=svc.date(2026, 4, 3),
            )

        self.assertEqual(converted, 965.0)
        rate_mock.assert_awaited_once()

    async def test_resolve_cost_amount_pair_prefers_fact_rub(self) -> None:
        rub, native, used_planned = await svc._resolve_cost_amount_pair(
            fact_rub=480.0,
            planned_native=5.0,
            currency_code="USD",
            fx_rate=96.0,
            calc_date=svc.date(2026, 4, 3),
        )

        self.assertEqual(rub, 480.0)
        self.assertEqual(native, 5.0)
        self.assertFalse(used_planned)

    async def test_normalize_cogs_amounts_treats_usd_source_as_native(self) -> None:
        rub, native = await svc._normalize_cogs_amounts(
            374.0,
            currency_code="USD",
            fx_rate=96.5,
            calc_date=svc.date(2026, 4, 4),
        )

        self.assertEqual(native, 374.0)
        self.assertEqual(rub, 36091.0)

    async def test_normalize_cogs_amounts_treats_rub_order_source_as_rub_for_usd_store(self) -> None:
        rub, native = await svc._normalize_cogs_amounts(
            112500.0,
            currency_code="USD",
            fx_rate=79.7293,
            calc_date=svc.date(2026, 4, 4),
            source_currency="RUB",
        )

        self.assertEqual(rub, 112500.0)
        self.assertEqual(native, 1411.0246)

    def test_planned_costs_use_only_actual_market_boost(self) -> None:
        ctx = {
            "path_map": {},
            "category_settings": {},
            "store_settings": {"target_drr_percent": 12.0},
            "logistics_store": {},
            "logistics_product": {},
        }

        no_actual = svc._planned_costs_for_row(
            {
                "sku": "SKU-1",
                "sale_price": 1000.0,
                "strategy_boost_bid_percent": 8.0,
                "strategy_market_boost_bid_percent": None,
            },
            ctx,
        )
        with_actual = svc._planned_costs_for_row(
            {
                "sku": "SKU-1",
                "sale_price": 1000.0,
                "strategy_boost_bid_percent": 8.0,
                "strategy_market_boost_bid_percent": 5.0,
            },
            ctx,
        )

        self.assertFalse(no_actual["ads_from_strategy"])
        self.assertIsNone(no_actual["ads"])
        self.assertEqual(no_actual["ads_rate_percent"], 0.0)
        self.assertEqual(no_actual["ads_source"], "none")
        self.assertEqual(with_actual["ads"], 50.0)
        self.assertTrue(with_actual["ads_from_strategy"])
        self.assertEqual(with_actual["ads_rate_percent"], 5.0)
        self.assertEqual(with_actual["ads_source"], "market_boost_fact")

    def test_strategy_snapshot_is_complete_requires_installed_price(self) -> None:
        self.assertFalse(
            svc._strategy_snapshot_is_complete(
                {
                    "captured_at": "2026-04-13T07:00:00+00:00",
                    "installed_price": None,
                    "decision_label": "Умеренно",
                }
            )
        )
        self.assertTrue(
            svc._strategy_snapshot_is_complete(
                {
                    "captured_at": "2026-04-13T07:00:00+00:00",
                    "installed_price": 122022.0,
                    "decision_label": "Умеренно",
                }
            )
        )

    async def test_tracking_delivery_days_prefers_order_created_date(self) -> None:
        row = {
            "item_status": "Доставлен покупателю",
            "order_id": "1",
            "sku": "SKU-1",
            "order_created_at": "2026-04-01T23:30:00+00:00",
            "order_created_date": "2026-04-01",
            "delivery_date": "2026-04-08",
            "sale_price": 1000.0,
            "sale_price_with_coinvest": 900.0,
            "profit": 100.0,
            "ads": 0.0,
            "cogs_price": 500.0,
        }

        with patch.object(svc, "_catalog_marketplace_stores_context", return_value=[{"platform": "yandex_market", "store_uid": "yandex_market:1", "store_id": "1"}]), \
             patch.object(svc, "get_pricing_store_settings", return_value={}), \
             patch.object(svc, "_load_sales_overview_order_fact_rows", return_value=[row]), \
             patch.object(svc, "_load_extra_ads_scope", return_value={}), \
             patch.object(svc, "_load_netting_scope", return_value=[]):
            payload = await svc.get_sales_overview_tracking(store_id="1", date_mode="delivery")

        self.assertEqual(payload["active_month_key"], "2026-04")
        month = payload["years"][0]["months"][0]
        self.assertEqual(month["delivery_time_days"], 7.0)
        self.assertEqual(month["days"][0]["delivery_time_days"], 7.0)

    async def test_tracking_delivery_mode_counts_returns_and_day_level_costs(self) -> None:
        delivered_row = {
            "item_status": "Доставлен покупателю",
            "order_id": "1",
            "sku": "SKU-1",
            "order_created_at": "2026-04-01T12:00:00+03:00",
            "order_created_date": "2026-04-01",
            "delivery_date": "2026-04-07",
            "sale_price": 1000.0,
            "sale_price_with_coinvest": 900.0,
            "profit": 100.0,
            "ads": 10.0,
            "cogs_price": 500.0,
        }
        return_row = {
            "item_status": "Возврат",
            "order_id": "2",
            "sku": "SKU-2",
            "order_created_at": "2026-04-02T12:00:00+03:00",
            "order_created_date": "2026-04-02",
            "delivery_date": "2026-04-07",
            "sale_price": 0.0,
            "sale_price_with_coinvest": 0.0,
            "profit": 0.0,
            "ads": 0.0,
            "cogs_price": 0.0,
        }

        with patch.object(svc, "_catalog_marketplace_stores_context", return_value=[{"platform": "yandex_market", "store_uid": "yandex_market:1", "store_id": "1"}]), \
             patch.object(svc, "get_pricing_store_settings", return_value={}), \
             patch.object(svc, "_load_sales_overview_order_fact_rows", return_value=[delivered_row, return_row]), \
             patch.object(svc, "_load_extra_ads_scope", return_value={"2026-04-07": 50.0}), \
             patch.object(svc, "_load_netting_scope", return_value=[{"transactionType": "Удержание", "offerOrServiceName": "Отмена заказа по вине продавца", "transactionDate": "2026-04-07T10:00:00+03:00", "transactionSum": "-25"}]):
            payload = await svc.get_sales_overview_tracking(store_id="1", date_mode="delivery")

        month = payload["years"][0]["months"][0]
        day = month["days"][0]
        self.assertEqual(day["date"], "2026-04-07")
        self.assertEqual(day["revenue"], 1000.0)
        self.assertEqual(day["returns_pct"], 50.0)
        self.assertEqual(day["ads_amount"], 60.0)
        self.assertEqual(day["operational_errors"], 25.0)
        self.assertEqual(day["profit_amount"], 25.0)
        self.assertEqual(month["returns_pct"], 50.0)
        self.assertEqual(month["operational_errors"], 25.0)

    async def test_live_open_row_does_not_keep_stale_delivery_date_from_history(self) -> None:
        historical = {
            "order_id": "10",
            "sku": "SKU-10",
            "order_created_at": "2026-04-05T10:00:00+03:00",
            "order_created_date": "2026-04-05",
            "shipment_date": "2026-04-06",
            "delivery_date": "2026-04-07",
            "item_status": "Доставлен покупателю",
        }
        live = {
            "order_id": "10",
            "sku": "SKU-10",
            "order_created_at": "2026-04-05T10:00:00+03:00",
            "order_created_date": "2026-04-05",
            "shipment_date": "",
            "delivery_date": "",
            "item_status": "Оформлен",
            "item_name": "Item",
            "sale_price": 1000.0,
            "payment_price": 900.0,
            "subsidy_amount": 100.0,
            "payload_json": "",
            "source_updated_at": "2026-04-08T12:00:00+03:00",
            "loaded_at": "2026-04-08T12:00:00+03:00",
            "store_uid": "yandex_market:1",
            "platform": "yandex_market",
        }

        with patch.object(svc, "_load_orders_scope", return_value=([historical], [], "2026-04-05", "2026-04-07", "")), \
             patch.object(svc, "_load_live_current_month_orders", return_value=[live]), \
             patch.object(svc, "get_pricing_store_settings", return_value={}), \
             patch.object(svc, "get_sales_overview_cogs_source_map", return_value={"rows": []}), \
             patch.object(svc, "_catalog_marketplace_stores_context", return_value=[{"store_uid": "yandex_market:1", "currency_code": "RUB"}]), \
             patch.object(svc, "_snapshot_fallback_metrics", return_value=({}, {}, {})), \
             patch.object(svc, "get_sales_overview_order_rows_map", return_value={}), \
             patch.object(svc, "get_pricing_strategy_results_map", return_value={"yandex_market:1": {}}), \
             patch.object(svc, "_load_strategy_iteration_snapshot_map", return_value={}), \
             patch.object(svc, "_load_strategy_snapshot_map", return_value={}), \
             patch.object(svc, "_load_actual_market_boost_map", return_value={}), \
             patch.object(svc, "_planned_cost_context", return_value={"path_map": {}, "category_settings": {}, "store_settings": {}, "logistics_store": {}, "logistics_product": {}}), \
             patch.object(svc, "_build_netting_delivery_map", return_value=({}, {})), \
             patch.object(svc, "_load_netting_scope", return_value=[]), \
             patch.object(svc, "replace_sales_overview_order_rows", return_value=0), \
             patch.object(svc, "replace_sales_overview_order_rows_hot", return_value=1):
            result = await svc._build_sales_overview_order_rows_for_store(store_uid="yandex_market:1")

        row = result["rows"][0]
        self.assertEqual(row["item_status"], "Оформлен")
        self.assertEqual(row["delivery_date"], "")
        self.assertEqual(row["shipment_date"], "")


if __name__ == "__main__":
    unittest.main()
