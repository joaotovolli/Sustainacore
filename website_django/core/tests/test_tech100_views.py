from datetime import datetime
from unittest import mock

from django.test import SimpleTestCase
from django.urls import reverse


class Tech100ViewTests(SimpleTestCase):
    @mock.patch("core.views.fetch_tech100")
    def test_tech100_view_renders_table_headers_and_details(self, fetch_mock):
        fetch_mock.return_value = {
            "items": [
                {
                    "port_date": "2025-01-01",
                    "rank_index": 1,
                    "company_name": "Example Corp",
                    "ticker": "EXM",
                    "gics_sector": "Software",
                    "aiges_composite_average": 88.2,
                    "summary": "Sample summary.",
                    "transparency": 70,
                }
            ],
            "error": None,
            "meta": {},
        }

        response = self.client.get(reverse("tech100"))

        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        for header in ["Port Date", "Rank Index", "Company", "AIGES Composite", "Details"]:
            self.assertIn(header, content)
        self.assertIn("View details", content)
        self.assertIn("Transparency", content)
        self.assertIn("2025-01-01", content)
        self.assertIn("Sample summary.", content)

    @mock.patch("core.views.fetch_tech100")
    def test_tech100_groups_history_and_uses_latest_row(self, fetch_mock):
        fetch_mock.return_value = {
            "items": [
                {
                    "port_date": "2024-12-01",
                    "rank_index": 2,
                    "company_name": "Example Corp",
                    "ticker": "EXM",
                    "gics_sector": "Software",
                    "aiges_composite_average": 80,
                    "transparency": 70,
                    "ethical_principles": 65,
                    "governance_structure": 60,
                    "regulatory_alignment": 55,
                    "stakeholder_engagement": 50,
                },
                {
                    "port_date": "2025-01-01",
                    "rank_index": 1,
                    "company_name": "Example Corp",
                    "ticker": "EXM",
                    "gics_sector": "Software",
                    "aiges_composite_average": 95,
                    "transparency": 90,
                    "ethical_principles": 85,
                    "governance_structure": 80,
                    "regulatory_alignment": 75,
                    "stakeholder_engagement": 70,
                    "summary": "Latest snapshot summary",
                },
            ],
            "error": None,
            "meta": {},
        }

        response = self.client.get(reverse("tech100"))

        self.assertEqual(response.status_code, 200)
        companies = response.context["companies"]
        self.assertEqual(len(companies), 1)
        latest = companies[0]
        self.assertIsInstance(latest["port_date"], datetime)
        self.assertEqual(latest["port_date"].date().isoformat(), "2025-01-01")
        self.assertEqual(latest["rank_index"], 1)
        self.assertEqual(latest["aiges_composite"], 95)
        history = latest.get("history") or []
        self.assertEqual(len(history), 2)
        self.assertEqual(
            [row["port_date"].date().isoformat() for row in history],
            ["2024-12-01", "2025-01-01"],
        )
        content = response.content.decode("utf-8")
        self.assertIn("2025-01-01", content)
        self.assertIn("2024-12-01", content)
        self.assertIn("95.0", content)
        self.assertIn("90.0", content)
        self.assertIn("Latest governance snapshot", content)

    @mock.patch("core.views.fetch_tech100")
    def test_tech100_maps_alternate_field_names(self, fetch_mock):
        fetch_mock.return_value = {
            "items": [
                {
                    "updated_at": "2025-02-01",
                    "rank": 3,
                    "company": "Alt Corp",
                    "symbol": "ALT",
                    "sector": "Software",
                    "overall": 91.2,
                    "transparency_score": 71,
                    "ethics": 66,
                    "governance": 61,
                    "regulatory_alignment": 56,
                    "stakeholder": 51,
                    "summary": "Alt naming snapshot",
                },
                {
                    "port_date": "2025-01-01",
                    "rank_index": 4,
                    "company_name": "Alt Corp",
                    "ticker": "ALT",
                    "gics_sector": "Software",
                    "aiges_composite_average": 88.8,
                    "transparency": 70,
                    "ethical_principles": 65,
                    "governance_structure": 60,
                    "regulatory_alignment": 55,
                    "stakeholder_engagement": 50,
                },
            ],
            "error": None,
            "meta": {},
        }

        response = self.client.get(reverse("tech100"))
        self.assertEqual(response.status_code, 200)
        companies = response.context["companies"]
        self.assertEqual(len(companies), 1)
        latest = companies[0]
        self.assertEqual(latest["sector"], "Software")
        self.assertEqual(latest["aiges_composite"], 91.2)
        self.assertEqual(latest["summary"], "Alt naming snapshot")
        history = latest.get("history") or []
        self.assertEqual(
            [row["port_date_str"] for row in history],
            ["2025-01-01", "2025-02-01"],
        )
        content = response.content.decode("utf-8")
        self.assertIn("91.2", content)
        self.assertIn("71.0", content)
        self.assertIn("61.0", content)


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
            reverse("tech100_export"), {"port_date": "2025-01-01", "sector": "Software", "q": "EXM"}
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv")
        content = response.content.decode("utf-8")
        lines = [line for line in content.splitlines() if line.strip()]
        self.assertEqual(
            lines[0],
            "PORT_DATE,RANK_INDEX,COMPANY_NAME,TICKER,PORT_WEIGHT,GICS_SECTOR,TRANSPARENCY,ETHICAL_PRINCIPLES,GOVERNANCE_STRUCTURE,REGULATORY_ALIGNMENT,STAKEHOLDER_ENGAGEMENT,AIGES_COMPOSITE_AVERAGE,SUMMARY",
        )
        self.assertIn("Example Corp", content)
        # Filter should remove the non-matching ticker
        self.assertNotIn("Other Corp", content)
