class RecommendationService:
    def __init__(self, repository):
        self.repository = repository

    def _stage_label(self, profile):
        if not profile:
            return "游客"
        return "老用户" if profile.get("user_type") == "old" else "新用户"

    def member_profile(self, user_id):
        profile = self.repository.get_member_profile(user_id)
        if not profile:
            return None
        profile = dict(profile)
        profile["user_stage_label"] = self._stage_label(profile)
        return profile

    def dashboard(self):
        return {
            "summary": self.repository.get_rating_summary(),
            "distribution": self.repository.get_rating_distribution(),
            "popular_meals": self.repository.get_popular_meals(10),
            "realtime_top_meals": self.repository.get_realtime_top_meals(10)
            if hasattr(self.repository, "get_realtime_top_meals")
            else [],
            "active_users": self.repository.get_active_users(10)
            if hasattr(self.repository, "get_active_users")
            else [],
            "metrics": self.repository.get_model_metrics(),
            "latest_reviews": self.repository.get_latest_reviews(10)
            if hasattr(self.repository, "get_latest_reviews")
            else [],
            "live_review_count": self.repository.get_live_review_count(10)
            if hasattr(self.repository, "get_live_review_count")
            else 0,
        }

    def visualization(self):
        return {
            "summary": self.repository.get_rating_summary(),
            "distribution": self.repository.get_rating_distribution(),
            "popular_meals": self.repository.get_popular_meals(10),
            "realtime_top_meals": self.repository.get_realtime_top_meals(10)
            if hasattr(self.repository, "get_realtime_top_meals")
            else [],
            "active_users": self.repository.get_active_users(10)
            if hasattr(self.repository, "get_active_users")
            else [],
            "model_metrics": self.repository.get_model_metrics(),
        }

    def user_recommendations(self, user_id):
        user_id = (user_id or "").strip()
        profile = self.repository.get_user_profile(user_id) if user_id else None
        als_rows = self.repository.get_als_user_recommendations(user_id, 10) if user_id else []
        itemcf_rows = (
            self.repository.get_itemcf_user_recommendations(user_id, 10) if user_id else []
        )

        if not profile and not als_rows and not itemcf_rows:
            return {
                "mode": "cold_start",
                "message": "新用户或无历史行为用户，使用热门高评分菜品进行冷启动推荐。",
                "profile": None,
                "als": [],
                "itemcf": [],
                "fallback": self.repository.get_popular_meals(10),
            }

        fallback = []
        message = "已根据用户历史行为展示 ALS 主推荐与 ItemCF 辅助推荐。"
        if not itemcf_rows:
            fallback = self.repository.get_popular_meals(10)
            message = "该用户 ItemCF 证据不足，使用 ALS 主推荐并补充热门菜品兜底。"

        return {
            "mode": "personalized",
            "message": message,
            "profile": profile,
            "als": als_rows,
            "itemcf": itemcf_rows,
            "fallback": fallback,
        }

    def member_recommendations(self, user_id):
        profile = self.member_profile(user_id)
        history = self.repository.get_user_history(user_id, 20)

        if not profile or profile.get("user_type") == "new" or not history:
            return {
                "strategy": "cold_start_popular",
                "message": "新注册用户或暂无历史点餐行为，系统使用人气最高的菜品进行冷启动推荐。",
                "profile": profile,
                "history": history,
                "als": [],
                "itemcf": [],
                "fallback": self.repository.get_popular_meals(10),
                "top_rated_meals": self.repository.get_popular_meals(12),
                "realtime_top_meals": self.repository.get_realtime_top_meals(12)
                if hasattr(self.repository, "get_realtime_top_meals")
                else [],
            }

        als_rows = self.repository.get_als_user_recommendations(user_id, 10)
        itemcf_rows = self.repository.get_itemcf_user_recommendations(user_id, 10)
        fallback = []
        strategy = "personalized_hybrid"
        message = "基于历史点餐/评分行为，系统同时展示 ALS 主推荐和 ItemCF 辅助推荐。"

        if not als_rows and not itemcf_rows:
            strategy = "history_fallback_popular"
            message = "该老用户暂无离线模型推荐结果，使用热门菜品作为兜底推荐。"
            fallback = self.repository.get_popular_meals(10)
        elif not itemcf_rows:
            strategy = "als_with_popular_fallback"
            message = "该用户 ItemCF 共现证据不足，使用 ALS 推荐并补充热门菜品兜底。"
            fallback = self.repository.get_popular_meals(10)

        return {
            "strategy": strategy,
            "message": message,
            "profile": profile,
            "history": history,
            "als": als_rows,
            "itemcf": itemcf_rows,
            "fallback": fallback,
            "top_rated_meals": self.repository.get_popular_meals(12),
            "realtime_top_meals": self.repository.get_realtime_top_meals(12)
            if hasattr(self.repository, "get_realtime_top_meals")
            else [],
        }

    def meal_similarities(self, meal_id):
        meal_id = (meal_id or "").strip()
        return {
            "meal_id": meal_id,
            "als": self.repository.get_als_meal_similarities(meal_id, 10) if meal_id else [],
            "itemcf": self.repository.get_itemcf_meal_similarities(meal_id, 10) if meal_id else [],
        }

    def database_status(self):
        if hasattr(self.repository, "table_counts"):
            return self.repository.table_counts()
        return []
