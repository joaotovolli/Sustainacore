from unittest import mock

from django.test import TestCase

from ai_reg.data import AiRegDataError


class HomeViewTests(TestCase):
    def test_home_renders_when_ai_reg_dates_unavailable(self):
        with mock.patch("core.views.ai_reg_data.fetch_as_of_dates", side_effect=AiRegDataError("offline")):
            response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
