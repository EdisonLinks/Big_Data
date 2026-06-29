import unittest
from datetime import date, timedelta

from werkzeug.security import generate_password_hash

from webapp.app import create_app


class RouteRepository:
    def __init__(self):
        self.review_events = []
        self.members = {
            "old": {
                "user_id": "old-user",
                "username": "old",
                "password_hash": generate_password_hash("secret"),
                "register_date": date.today() - timedelta(days=500),
                "user_type": "old",
            }
        }

    def get_member_by_username(self, username):
        return self.members.get(username)

    def get_member_by_user_id(self, user_id):
        for member in self.members.values():
            if member["user_id"] == user_id:
                return member
        return None

    def create_member(self, username, password_hash):
        member = {
            "user_id": f"user-{username}",
            "username": username,
            "password_hash": password_hash,
            "register_date": date.today(),
            "user_type": "new",
        }
        self.members[username] = member
        return member

    def get_member_profile(self, user_id):
        member = self.get_member_by_user_id(user_id)
        if not member:
            return None
        return {
            "user_id": user_id,
            "username": member["username"],
            "register_date": member["register_date"],
            "user_type": member["user_type"],
            "rating_count": 1 if user_id == "old-user" else 0,
            "avg_rating": 4.5 if user_id == "old-user" else None,
        }

    def get_user_history(self, user_id, limit=20):
        if user_id == "old-user":
            return [{"meal_id": "M1", "meal_name": "菜品 M1", "rating": 5.0, "review": "好吃"}]
        return []

    def get_user_profile(self, user_id):
        return {"user_id": user_id, "rating_count": 1} if user_id == "old-user" else None

    def user_has_ordered_meal(self, user_id, meal_id):
        return user_id == "old-user" and meal_id == "M1"

    def get_als_user_recommendations(self, user_id, limit=10):
        return [{"meal_id": "ALS1", "rank_no": 1, "predicted_score": 4.8}] if user_id == "old-user" else []

    def get_itemcf_user_recommendations(self, user_id, limit=10):
        return [{"meal_id": "ICF1", "rank_no": 1, "predicted_score": 4.7, "evidence_count": 2}] if user_id == "old-user" else []

    def get_popular_meals(self, limit=10):
        return [{"meal_id": "HOT1", "meal_name": "热门菜品", "rating_count": 100, "avg_rating": 4.9}]

    def get_realtime_top_meals(self, limit=10):
        return [
            {
                "meal_id": "M1",
                "meal_name": "菜品 M1",
                "review_count_2s": 6,
                "avg_rating_2s": 4.83,
                "hot_score_2s": 38.6,
                "window_start": "2026-06-27 10:00:00",
                "window_end": "2026-06-27 10:05:00",
                "updated_at": "2026-06-27 10:05:02",
            }
        ][:limit]

    def get_realtime_meal_stats(self, meal_id):
        return {
            "meal_id": meal_id,
            "review_count_2s": 6,
            "avg_rating_2s": 4.83,
            "hot_score_2s": 38.6,
            "window_start": "2026-06-27 10:00:00",
            "window_end": "2026-06-27 10:05:00",
            "updated_at": "2026-06-27 10:05:02",
        }

    def search_meals(self, query, limit=30):
        rows = [
            {
                "meal_id": "M1",
                "meal_name": "菜品 M1",
                "category": "测试分类",
                "avg_rating": 5.0,
                "rating_count": 1,
                "popularity_rank": 1,
            }
        ]
        return rows if query in {"M1", "菜品"} else []

    def get_meal(self, meal_id):
        return {"meal_id": meal_id, "meal_name": f"菜品 {meal_id}", "avg_rating": 4.6, "rating_count": 12}

    def get_meal_review_stats(self, meal_id):
        return {"avg_rating": 5.0, "rating_count": 1}

    def get_meal_reviews(self, meal_id, limit=30):
        return [{"user_id": "old-user", "rating": 5.0, "review": "好吃", "review_date": "2017-06-01"}]

    def get_als_meal_similarities(self, meal_id, limit=10):
        return [{"similar_meal_id": "SIM1", "rank_no": 1, "similarity": 0.9}]

    def get_latest_reviews(self, limit=10):
        return [
            {
                "user_id": "old-user",
                "meal_id": "M1",
                "meal_name": "Meal M1",
                "rating": 5.0,
                "review": "good",
                "review_date": "2017-06-01",
                "source": "replay",
            },
            {
                "user_id": "old-user",
                "meal_id": "M2",
                "meal_name": "Meal M2",
                "rating": 4.0,
                "review": "nice",
                "review_date": "2026-06-26",
                "source": "flask_review_form",
            },
        ]

    def get_live_review_count(self, since_minutes=10):
        return 7

    def publish_review_event(self, event):
        self.review_events.append(event)

    def get_itemcf_meal_similarities(self, meal_id, limit=10):
        return [{"similar_meal_id": "SIM2", "rank_no": 1, "similarity": 0.8, "common_users": 3}]

    def get_rating_summary(self):
        return {"rating_count": 1, "user_count": 1, "meal_count": 1, "avg_rating": 5.0}

    def get_rating_distribution(self):
        return [{"rating": 5.0, "rating_count": 10, "ratio": 0.5}]

    def get_active_users(self, limit=10):
        return [{"user_id": "old-user", "rating_count": 12, "avg_rating": 4.6, "activity_level": "中活跃"}]

    def get_model_metrics(self):
        return [{"model_type": "als", "rmse": 1.13, "prediction_coverage": 0.99, "mae": None}]

    def table_counts(self):
        return []


class WebRoutesTest(unittest.TestCase):
    def create_client(self):
        app = create_app(RouteRepository())
        app.config.update(TESTING=True, SECRET_KEY="test-secret")
        return app.test_client()

    def test_register_logs_in_new_user_and_shows_cold_start_home(self):
        client = self.create_client()

        response = client.post(
            "/register",
            data={"username": "fresh", "password": "secret"},
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("冷启动", response.get_data(as_text=True))
        self.assertIn("HOT1", response.get_data(as_text=True))

    def test_home_shows_database_backed_top_rated_marquee(self):
        client = self.create_client()

        response = client.post(
            "/login",
            data={"username": "old", "password": "secret"},
            follow_redirects=True,
        )

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("高分菜品速递", body)
        self.assertIn("HOT1", body)
        self.assertIn("home-marquee.js", body)

    def test_home_shows_flink_realtime_window_meals(self):
        client = self.create_client()

        response = client.post(
            "/login",
            data={"username": "old", "password": "secret"},
            follow_redirects=True,
        )

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Flink 2 秒实时热度", body)
        self.assertIn("近 2 秒 6 次", body)

    def test_root_shows_login_first_and_hides_admin_navigation(self):
        client = self.create_client()

        response = client.get("/")
        body = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("用户登录", body)
        self.assertNotIn("模型对比", body)
        self.assertNotIn("数据库状态", body)

    def test_login_existing_user_shows_history_and_hybrid_recommendations(self):
        client = self.create_client()

        response = client.post(
            "/login",
            data={"username": "old", "password": "secret"},
            follow_redirects=True,
        )

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("M1", body)
        self.assertIn("ALS1", body)
        self.assertIn("ICF1", body)
        self.assertNotIn("模型对比", body)
        self.assertNotIn("数据库状态", body)

    def test_search_returns_matching_meals_for_logged_in_user(self):
        client = self.create_client()
        client.post("/login", data={"username": "old", "password": "secret"})

        response = client.get("/search?q=M1")
        body = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("搜索结果", body)
        self.assertIn("菜品 M1", body)
        self.assertIn("/meal/M1", body)

    def test_meal_detail_shows_reviews_and_two_similarity_sources(self):
        client = self.create_client()

        response = client.get("/meal/M1")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("好吃", body)
        self.assertIn("SIM1", body)
        self.assertIn("SIM2", body)

    def test_meal_detail_uses_live_review_stats_instead_of_meal_snapshot(self):
        client = self.create_client()

        response = client.get("/meal/M1")

        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("5.00", body)
        self.assertNotIn("4.60", body)
        self.assertIn("评分次数</span><strong>1</strong>", body)
        self.assertIn("近 2 秒实时评分", body)
        self.assertIn("4.83", body)

    def test_history_allows_logged_in_user_to_submit_review_event_for_ordered_meal(self):
        repo = RouteRepository()
        app = create_app(repo)
        app.config.update(TESTING=True, SECRET_KEY="test-secret")
        client = app.test_client()
        client.post("/login", data={"username": "old", "password": "secret"})

        response = client.post(
            "/review",
            data={"meal_id": "M1", "rating": "4", "review": "new review"},
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(len(repo.review_events), 1)
        event = repo.review_events[0]
        self.assertEqual(event["user_id"], "old-user")
        self.assertEqual(event["meal_id"], "M1")
        self.assertEqual(event["rating"], 4.0)
        self.assertEqual(event["review"], "new review")

    def test_review_submission_rejects_meals_not_in_user_history(self):
        repo = RouteRepository()
        app = create_app(repo)
        app.config.update(TESTING=True, SECRET_KEY="test-secret")
        client = app.test_client()
        client.post("/login", data={"username": "old", "password": "secret"})

        response = client.post(
            "/review",
            data={"meal_id": "M2", "rating": "4", "review": "new review"},
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(repo.review_events, [])

    def test_dashboard_api_returns_latest_reviews_for_live_display(self):
        client = self.create_client()

        response = client.get("/api/dashboard")
        data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["latest_reviews"][0]["meal_id"], "M1")

    def test_dashboard_includes_live_review_count(self):
        client = self.create_client()

        response = client.get("/api/dashboard")
        data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["live_review_count"], 7)

    def test_visualization_page_and_api_expose_chart_data(self):
        client = self.create_client()

        page = client.get("/visualization")
        body = page.get_data(as_text=True)
        self.assertEqual(page.status_code, 200)
        self.assertIn("数据图表", body)
        self.assertIn("echarts", body)

        response = client.get("/api/visualization")
        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["distribution"][0]["rating"], 5.0)
        self.assertEqual(data["popular_meals"][0]["meal_id"], "HOT1")
        self.assertEqual(data["active_users"][0]["user_id"], "old-user")
        self.assertEqual(data["model_metrics"][0]["model_type"], "als")
        self.assertEqual(data["realtime_top_meals"][0]["meal_id"], "M1")

    def test_review_route_tolerates_kafka_failure(self):
        class FailingRepo(RouteRepository):
            def publish_review_event(self, event):
                raise RuntimeError("kafka unreachable")

        app = create_app(FailingRepo())
        app.config.update(TESTING=True, SECRET_KEY="test-secret")
        client = app.test_client()
        client.post("/login", data={"username": "old", "password": "secret"})

        response = client.post(
            "/review",
            data={"meal_id": "M1", "rating": "4", "review": "x"},
            follow_redirects=True,
        )

        # Kafka 不可达：仍返回 200（重定向后的页面），不抛 500，flash 含"暂不可用"
        self.assertEqual(response.status_code, 200)
        self.assertIn("暂不可用", response.get_data(as_text=True))

    def test_review_event_carries_event_time_and_source(self):
        repo = RouteRepository()
        app = create_app(repo)
        app.config.update(TESTING=True, SECRET_KEY="test-secret")
        client = app.test_client()
        client.post("/login", data={"username": "old", "password": "secret"})

        client.post("/review", data={"meal_id": "M1", "rating": "5", "review": "ok"})

        event = repo.review_events[-1]
        self.assertEqual(event["source"], "flask_review_form")
        self.assertIn("event_time", event)
        self.assertGreater(event["event_time"], 0)


if __name__ == "__main__":
    unittest.main()
