from unittest import mock

from django.test import SimpleTestCase
from django.urls import reverse


class Tech100ViewTests(SimpleTestCase):
    @mock.patch("core.views.fetch_tech100")
    def test_tech100_view_renders(self, fetch_mock):
        fetch_mock.return_value = {
            "items": [
                {
                    "port_date": "2025-01-01",
                    "rank_index": 1,
                    "company_name": "Example Corp",
                    "ticker": "EXM",
                    "gics_sector": "Software",
                    "aiges_composite_average": 88.2,
                }
            ],
            "error": None,
            "meta": {},
        }

        response = self.client.get(reverse("tech100"))

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Example Corp", response.content)
        self.assertIn(b"tech100-data", response.content)


class Tech100ExportTests(SimpleTestCase):
    @mock.patch("core.views.fetch_tech100")
    def test_export_returns_csv(self, fetch_mock):
        fetch_mock.return_value = {
            "items": [
                {
                    "port_date": "2025-01-01",
                    "rank_index": 1,
                    "company_name": "Example Corp",
                    "ticker": "EXM",
                    "gics_sector": "Software",
                    "port_weight": 2.5,
                    "transparency": 90,
                    "ethical_principles": 85,
                    "governance_structure": 80,
                    "regulatory_alignment": 75,
                    "stakeholder_engagement": 70,
                    "aiges_composite_average": 82,
                    "summary": "Sample summary",
                    "source_links": "https://example.com",
                },
                {
                    "port_date": "2025-01-01",
                    "rank_index": 2,
                    "company_name": "Other Corp",
                    "ticker": "OTR",
                    "gics_sector": "Software",
                    "port_weight": 1.2,
                    "summary": "Another summary",
                },
            ],
            "error": None,
            "meta": {},
        }

        response = self.client.get(
            reverse("tech100_export"), {"port_date": "2025-01-01", "sector": "Software", "search": "EXM"}
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv")
        content = response.content.decode("utf-8")
        lines = [line for line in content.splitlines() if line.strip()]
        self.assertTrue(lines[0].startswith("PORT_DATE"))
        self.assertIn("Example Corp", content)
        # Filter should remove the non-matching ticker
        self.assertNotIn("Other Corp", content)
