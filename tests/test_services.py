import unittest

from webapp.services import RecommendationService


class FakeRepository:
    def __init__(self):
        self.popular = [
            {"meal_id": "M1", "rating_count": 99, "avg_rating": 4.8},
            {"meal_id": "M2", "rating_count": 80, "avg_rating": 4.5},
        ]

    def get_user_profile(self, user_id):
        if user_id == "known":
            return {"user_id": "known", "rating_count": 12, "avg_rating": 4.2}
        return None

    def get_member_profile(self, user_id):
        if user_id == "new-user":
            return {"user_id": "new-user", "username": "new", "user_type": "new", "rating_count": 0}
        return None

    def get_user_history(self, user_id, limit=20):
        return []

    def get_als_user_recommendations(self, user_id, limit=10):
        if user_id == "known":
            return [{"meal_id": "A1", "rank_no": 1, "predicted_rating": 4.6}]
        return []

    def get_itemcf_user_recommendations(self, user_id, limit=10):
        if user_id == "known":
            return [{"meal_id": "I1", "rank_no": 1, "predicted_score": 4.9, "evidence_count": 3}]
        return []

    def get_popular_meals(self, limit=10):
        return self.popular[:limit]

    def get_realtime_top_meals(self, limit=10):
        return [
            {
                "meal_id": "RT1",
                "meal_name": "实时热菜",
                "review_count_2s": 8,
                "avg_rating_2s": 4.75,
                "hot_score_2s": 42.5,
                "window_start": "2026-06-27 10:00:00",
                "window_end": "2026-06-27 10:05:00",
                "updated_at": "2026-06-27 10:05:03",
            }
        ][:limit]

    def get_realtime_meal_stats(self, meal_id):
        if meal_id != "RT1":
            return None
        return {
            "meal_id": "RT1",
            "review_count_2s": 8,
            "avg_rating_2s": 4.75,
            "hot_score_2s": 42.5,
            "window_start": "2026-06-27 10:00:00",
            "window_end": "2026-06-27 10:05:00",
            "updated_at": "2026-06-27 10:05:03",
        }

    def get_rating_summary(self):
        return {"rating_count": 100, "user_count": 10, "meal_count": 5, "avg_rating": 4.1}

    def get_rating_distribution(self):
        return [{"rating": 5.0, "rating_count": 60}, {"rating": 4.0, "rating_count": 40}]

    def get_model_metrics(self):
        return [{"model_type": "als", "prediction_coverage": 0.99}]

    def get_active_users(self, limit=10):
        return [{"user_id": "U1", "rating_count": 20, "avg_rating": 4.6}]


class RecommendationServiceTest(unittest.TestCase):
    def test_existing_user_returns_two_model_recommendations_without_fallback(self):
        service = RecommendationService(FakeRepository())

        result = service.user_recommendations("known")

        self.assertEqual(result["mode"], "personalized")
        self.assertEqual(result["als"][0]["meal_id"], "A1")
        self.assertEqual(result["itemcf"][0]["meal_id"], "I1")
        self.assertEqual(result["fallback"], [])

    def test_unknown_user_gets_cold_start_popular_meals(self):
        service = RecommendationService(FakeRepository())

        result = service.user_recommendations("new-user")

        self.assertEqual(result["mode"], "cold_start")
        self.assertEqual(result["fallback"][0]["meal_id"], "M1")
        self.assertIn("新用户", result["message"])

    def test_dashboard_payload_contains_dynamic_database_sections(self):
        service = RecommendationService(FakeRepository())

        result = service.dashboard()

        self.assertEqual(result["summary"]["rating_count"], 100)
        self.assertEqual(result["distribution"][0]["rating"], 5.0)
        self.assertEqual(result["metrics"][0]["model_type"], "als")

    def test_member_recommendations_include_top_rated_meals_for_home_marquee(self):
        service = RecommendationService(FakeRepository())

        result = service.member_recommendations("new-user")

        self.assertEqual(result["top_rated_meals"][0]["meal_id"], "M1")

    def test_member_recommendations_include_realtime_window_top_meals(self):
        service = RecommendationService(FakeRepository())

        result = service.member_recommendations("new-user")

        self.assertEqual(result["realtime_top_meals"][0]["meal_id"], "RT1")
        self.assertEqual(result["realtime_top_meals"][0]["review_count_2s"], 8)

    def test_visualization_payload_groups_chart_sections(self):
        service = RecommendationService(FakeRepository())

        result = service.visualization()

        self.assertEqual(result["distribution"][0]["rating"], 5.0)
        self.assertEqual(result["popular_meals"][0]["meal_id"], "M1")
        self.assertEqual(result["active_users"][0]["user_id"], "U1")
        self.assertEqual(result["model_metrics"][0]["model_type"], "als")
        self.assertEqual(result["realtime_top_meals"][0]["meal_id"], "RT1")


if __name__ == "__main__":
    unittest.main()
