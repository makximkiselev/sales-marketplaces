import unittest

from backend.services import pricing_prices_service as svc


class PricingPricesServiceTests(unittest.TestCase):
    def test_materialized_price_metrics_are_not_reused_without_source_updated(self) -> None:
        self.assertFalse(
            svc._can_reuse_materialized_price_metrics(
                db_rec={"source_updated_at": ""},
                src_updated="",
            )
        )
        self.assertFalse(
            svc._can_reuse_materialized_price_metrics(
                db_rec={"source_updated_at": "2026-04-13T07:07:01+00:00"},
                src_updated="",
            )
        )

    def test_materialized_price_metrics_are_reused_only_for_matching_source_updated(self) -> None:
        self.assertTrue(
            svc._can_reuse_materialized_price_metrics(
                db_rec={"source_updated_at": "2026-04-13T07:07:01+00:00"},
                src_updated="2026-04-13T07:07:01+00:00",
            )
        )
        self.assertFalse(
            svc._can_reuse_materialized_price_metrics(
                db_rec={"source_updated_at": "2026-04-12T07:07:01+00:00"},
                src_updated="2026-04-13T07:07:01+00:00",
            )
        )


if __name__ == "__main__":
    unittest.main()
