import unittest
from datetime import date, timedelta

from webapp.services import RecommendationService


class MemberRepository:
    def __init__(self):
        self.old_user = {
            "user_id": "old-user",
            "username": "old_user",
            "register_date": date.today() - timedelta(days=500),
            "user_type": "old",
            "rating_count": 6,
            "avg_rating": 4.4,
        }
        self.new_user = {
            "user_id": "new-user",
            "username": "new_user",
            "register_date": date.today(),
            "user_type": "new",
            "rating_count": 0,
            "avg_rating": None,
        }

    def get_member_by_username(self, username):
        if username == "old_user":
            return {"user_id": "old-user", "username": username, "password_hash": "hash"}
        return None

    def get_member_profile(self, user_id):
        if user_id == "old-user":
            return self.old_user
        if user_id == "new-user":
            return self.new_user
        return None

    def get_user_profile(self, user_id):
        return {"user_id": "old-user", "rating_count": 6, "avg_rating": 4.4} if user_id == "old-user" else None

    def get_als_user_recommendations(self, user_id, limit=10):
        return [{"meal_id": "ALS1", "rank_no": 1, "predicted_score": 4.8}] if user_id == "old-user" else []

    def get_itemcf_user_recommendations(self, user_id, limit=10):
        return [{"meal_id": "ICF1", "rank_no": 1, "predicted_score": 4.9, "evidence_count": 2}] if user_id == "old-user" else []

    def get_popular_meals(self, limit=10):
        return [{"meal_id": "HOT1", "rating_count": 100, "avg_rating": 4.7}]

    def get_user_history(self, user_id, limit=20):
        if user_id == "old-user":
            return [{"meal_id": "H1", "rating": 5.0, "review": "nice"}]
        return []


class MemberServiceTest(unittest.TestCase):
    def test_member_profile_marks_registered_more_than_one_year_as_old_user(self):
        service = RecommendationService(MemberRepository())

        profile = service.member_profile("old-user")

        self.assertEqual(profile["user_type"], "old")
        self.assertEqual(profile["user_stage_label"], "老用户")

    def test_new_registered_user_gets_popular_meals_cold_start(self):
        service = RecommendationService(MemberRepository())

        result = service.member_recommendations("new-user")

        self.assertEqual(result["strategy"], "cold_start_popular")
        self.assertEqual(result["fallback"][0]["meal_id"], "HOT1")
        self.assertEqual(result["history"], [])

    def test_old_user_gets_history_and_two_model_recommendations(self):
        service = RecommendationService(MemberRepository())

        result = service.member_recommendations("old-user")

        self.assertEqual(result["strategy"], "personalized_hybrid")
        self.assertEqual(result["history"][0]["meal_id"], "H1")
        self.assertEqual(result["als"][0]["meal_id"], "ALS1")
        self.assertEqual(result["itemcf"][0]["meal_id"], "ICF1")


if __name__ == "__main__":
    unittest.main()
