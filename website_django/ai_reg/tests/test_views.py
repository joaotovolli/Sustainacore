from unittest import mock

from django.test import TestCase


class AiRegViewsTests(TestCase):
    def test_ai_regulation_page_loads(self):
        with mock.patch("ai_reg.views.ai_reg_data.fetch_as_of_dates", return_value=["2025-01-15"]):
            response = self.client.get("/ai-regulation/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Global AI Regulation")

    def test_as_of_dates_endpoint(self):
        with mock.patch("ai_reg.views.ai_reg_data.fetch_as_of_dates", return_value=["2025-01-15"]):
            response = self.client.get("/ai-regulation/data/as-of-dates")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"as_of_dates": ["2025-01-15"]})

    def test_heatmap_endpoint(self):
        payload = [{"iso2": "US", "name": "United States", "instruments_count": 1}]
        with mock.patch("ai_reg.views.ai_reg_data.fetch_heatmap", return_value=payload):
            response = self.client.get("/ai-regulation/data/heatmap", {"as_of": "2025-01-15"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["as_of"], "2025-01-15")
        self.assertEqual(response.json()["jurisdictions"], payload)

    def test_heatmap_requires_as_of(self):
        response = self.client.get("/ai-regulation/data/heatmap")
        self.assertEqual(response.status_code, 400)

    def test_jurisdiction_endpoint(self):
        summary = {"iso2": "US", "name": "United States", "obligations_count": 3, "data_quality": {"snapshots_without_source": 0, "flag": False}}
        instruments = [{"title_english": "Act", "title_official": "Act", "instrument_type": "law", "status": "in_force"}]
        milestones = [{"milestone_type": "effective", "milestone_date": "2026-01-01"}]
        sources = [{"title": "Gazette", "url": "https://example.com"}]
        with (
            mock.patch("ai_reg.views.ai_reg_data.fetch_jurisdiction_summary", return_value=summary),
            mock.patch("ai_reg.views.ai_reg_data.fetch_jurisdiction_instruments", return_value=instruments),
            mock.patch("ai_reg.views.ai_reg_data.fetch_jurisdiction_timeline", return_value=milestones),
            mock.patch("ai_reg.views.ai_reg_data.fetch_jurisdiction_sources", return_value=sources),
        ):
            response = self.client.get("/ai-regulation/data/jurisdiction/US/", {"as_of": "2025-01-15"})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["jurisdiction"]["iso2"], "US")
        self.assertEqual(body["instruments"], instruments)
        self.assertEqual(body["milestones"], milestones)
        self.assertEqual(body["sources"], sources)

    def test_jurisdiction_instruments_endpoint(self):
        instruments = [{"title_english": "Act", "title_official": "Act", "instrument_type": "law", "status": "draft"}]
        with mock.patch("ai_reg.views.ai_reg_data.fetch_jurisdiction_instruments", return_value=instruments):
            response = self.client.get("/ai-regulation/data/jurisdiction/US/instruments", {"as_of": "2025-01-15"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["instruments"], instruments)

    def test_jurisdiction_timeline_endpoint(self):
        milestones = [{"milestone_type": "effective", "milestone_date": "2026-01-01"}]
        with mock.patch("ai_reg.views.ai_reg_data.fetch_jurisdiction_timeline", return_value=milestones):
            response = self.client.get("/ai-regulation/data/jurisdiction/US/timeline", {"as_of": "2025-01-15"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["milestones"], milestones)
