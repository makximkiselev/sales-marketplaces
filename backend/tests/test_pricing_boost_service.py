import unittest
from unittest.mock import AsyncMock, patch

from backend.services import pricing_boost_service as svc


class PricingBoostServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        svc.invalidate_boost_cache()

    async def test_boost_overview_exposes_plan_fact_and_effectiveness(self) -> None:
        base_payload = {
            "rows": [
                {
                    "sku": "SKU-1",
                    "name": "Item",
                    "tree_path": ["Electronics"],
                    "placements": {},
                    "mrc_price_by_store": {"store-1": 1000.0},
                    "mrc_with_boost_price_by_store": {"store-1": 1050.0},
                    "target_price_by_store": {"store-1": 1200.0},
                    "updated_at": "2026-04-08T12:00:00+03:00",
                }
            ],
            "stores": [
                {
                    "store_uid": "store-1",
                    "store_id": "1",
                    "label": "Store 1",
                }
            ],
            "total_count": 1,
        }
        strategy_map = {
            "store-1": {
                "SKU-1": {
                    "decision_label": "Boost",
                    "boost_share": 25.0,
                    "boost_bid_percent": 4.0,
                    "market_boost_bid_percent": 5.0,
                    "installed_price": 1050.0,
                    "on_display_price": 1050.0,
                }
            }
        }
        order_rows = [
            {
                "sku": "SKU-1",
                "item_status": "Оформлен",
                "sale_price": 1000.0,
                "profit": 120.0,
                "ads": 50.0,
                "strategy_market_boost_bid_percent": 5.0,
                "order_created_at": "2026-04-08T11:00:00+03:00",
            },
            {
                "sku": "SKU-1",
                "item_status": "Оформлен",
                "sale_price": 1000.0,
                "profit": 120.0,
                "ads": 50.0,
                "strategy_market_boost_bid_percent": 0.0,
                "order_created_at": "2026-04-08T10:00:00+03:00",
            },
            {
                "sku": "SKU-1",
                "item_status": "Отгружен",
                "sale_price": 1000.0,
                "profit": 120.0,
                "ads": 50.0,
                "strategy_market_boost_bid_percent": 5.0,
                "order_created_at": "2026-04-08T12:00:00+03:00",
            },
            {
                "sku": "SKU-1",
                "item_status": "Отменен",
                "sale_price": 1000.0,
                "profit": 120.0,
                "ads": 50.0,
                "strategy_market_boost_bid_percent": 5.0,
                "order_created_at": "2026-04-08T13:00:00+03:00",
            },
        ]

        with patch.object(svc, "get_prices_overview", AsyncMock(return_value=base_payload)), \
             patch.object(svc, "get_pricing_strategy_results_map", return_value=strategy_map), \
             patch.object(svc, "_load_order_rows_for_store_day", AsyncMock(return_value=order_rows)):
            payload = await svc.get_boost_overview(
                scope="all",
                report_date="2026-04-08",
                page=1,
                page_size=50,
            )

        row = payload["rows"][0]
        self.assertEqual(row["orders_count_by_store"]["store-1"], 3)
        self.assertEqual(row["boosted_orders_count_by_store"]["store-1"], 2)
        self.assertEqual(row["planned_boosted_orders_count_by_store"]["store-1"], 0.75)
        self.assertEqual(row["boost_effectiveness_pct_by_store"]["store-1"], 266.67)

    async def test_boost_overview_effectiveness_is_none_without_plan(self) -> None:
        base_payload = {
            "rows": [
                {
                    "sku": "SKU-1",
                    "name": "Item",
                    "tree_path": ["Electronics"],
                    "placements": {},
                    "updated_at": "2026-04-08T12:00:00+03:00",
                }
            ],
            "stores": [{"store_uid": "store-1", "store_id": "1", "label": "Store 1"}],
            "total_count": 1,
        }

        with patch.object(svc, "get_prices_overview", AsyncMock(return_value=base_payload)), \
             patch.object(svc, "get_pricing_strategy_results_map", return_value={"store-1": {"SKU-1": {"decision_label": "Boost"}}}), \
             patch.object(svc, "_load_order_rows_for_store_day", AsyncMock(return_value=[])):
            payload = await svc.get_boost_overview(
                scope="all",
                report_date="2026-04-09",
                page=1,
                page_size=50,
            )

        row = payload["rows"][0]
        self.assertIsNone(row["planned_boosted_orders_count_by_store"]["store-1"])
        self.assertIsNone(row["boost_effectiveness_pct_by_store"]["store-1"])


if __name__ == "__main__":
    unittest.main()
