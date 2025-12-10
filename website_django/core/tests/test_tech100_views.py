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
        self.assertIn("Only the latest rebalance is available", content)

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
        self.assertIn("History", content)

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

    @mock.patch("core.views.fetch_tech100")
    def test_tech100_maps_rank_and_esg_scores(self, fetch_mock):
        fetch_mock.return_value = {
            "items": [
                {
                    "port_date": "2024-01-01T00:00:00Z",
                    "rank": "10",
                    "company": "Data Corp",
                    "symbol": "DATA",
                    "gics_sector_name": "Information Technology",
                    "transparency_score": "55",
                    "ethics_score": 60,
                    "accountability_score": 65,
                    "regulation_alignment": 70,
                    "stakeholder_score": 75,
                    "aiges_score": 80,
                    "summary": "First snapshot",
                },
                {
                    "port_date": "2024-02-01",
                    "rank": "5",
                    "company": "Data Corp",
                    "symbol": "DATA",
                    "gics_sector_name": "Information Technology",
                    "transparency_score": 56,
                    "ethics": 61,
                    "governance": 66,
                    "regulatory": 71,
                    "stakeholder_engagement": 76,
                    "overall": 81,
                    "company_summary": "Latest snapshot summary",
                },
            ],
            "error": None,
            "meta": {},
        }

        response = self.client.get(reverse("tech100"))
        self.assertEqual(response.status_code, 200)
        companies = response.context["companies"]
        self.assertEqual(len(companies), 1)
        company = companies[0]
        # Latest row should carry mapped values
        self.assertEqual(company["rank_index"], 5.0)
        self.assertEqual(company["transparency"], 56)
        self.assertEqual(company["ethical_principles"], 61)
        self.assertEqual(company["governance_structure"], 66)
        self.assertEqual(company["regulatory_alignment"], 71)
        self.assertEqual(company["stakeholder_engagement"], 76)
        self.assertEqual(company["aiges_composite"], 81)
        self.assertEqual(company["summary"], "Latest snapshot summary")
        history = company.get("history") or []
        self.assertEqual(len(history), 2)
        self.assertEqual(
            [h.get("port_date_str") or h["port_date"].date().isoformat() for h in history],
            ["2024-01-01", "2024-02-01"],
        )
        content = response.content.decode("utf-8")
        for expected in ["Data Corp", "5", "81.0", "76.0", "71.0", "56.0", "Latest snapshot summary"]:
            self.assertIn(expected, content)

    @mock.patch("core.views.fetch_tech100")
    def test_tech100_derives_rank_when_missing(self, fetch_mock):
        fetch_mock.return_value = {
            "items": [
                {
                    "updated_at": "2025-03-01",
                    "company_name": "Alpha",
                    "ticker": "ALP",
                    "overall": 80,
                },
                {
                    "updated_at": "2025-03-01",
                    "company": "Beta",
                    "symbol": "BET",
                    "overall": 70,
                },
            ],
            "error": None,
            "meta": {},
        }

        response = self.client.get(reverse("tech100"))
        self.assertEqual(response.status_code, 200)
        companies = response.context["companies"]
        ranks = {c["company_name"]: c["rank_index"] for c in companies}
        self.assertEqual(ranks["Alpha"], 1)
        self.assertEqual(ranks["Beta"], 2)
        content = response.content.decode("utf-8")
        self.assertIn("Alpha", content)
        self.assertIn("Beta", content)
        self.assertIn("1", content)
        self.assertIn("2", content)


class HomeViewTests(SimpleTestCase):
    @mock.patch("core.views.fetch_news")
    @mock.patch("core.views.fetch_tech100")
    def test_home_renders_with_canonical_tech100_data(self, tech100_mock, news_mock):
        tech100_mock.return_value = {
            "items": [
                {
                    "company_name": "Acme Corp",
                    "ticker": "ACME",
                    "sector": "Technology",
                    "region": "North America",
                    "aiges_composite": 90,
                }
            ],
            "error": None,
            "meta": {},
        }
        news_mock.return_value = {
            "items": [
                {
                    "title": "Gov headline",
                    "summary": "News summary",
                    "source": "Source",
                    "published_at": "2025-01-01",
                    "tags": [],
                }
            ],
            "error": None,
            "meta": {},
        }

        response = self.client.get("/", HTTP_HOST="sustainacore.org")
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn("Acme Corp", content)
        self.assertIn("Technology", content)
        self.assertIn("90", content)
        self.assertIn("Gov headline", content)


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
