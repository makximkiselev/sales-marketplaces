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


if __name__ == "__main__":
    unittest.main()
