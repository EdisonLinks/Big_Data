from contextlib import contextmanager
from datetime import date, datetime
from uuid import uuid4

try:
    from .config import config
except ImportError:
    from config import config


class MySQLRepository:
    def __init__(self, db_config=config):
        self.config = db_config

    @contextmanager
    def connect(self):
        import pymysql

        conn = pymysql.connect(
            host=self.config.DB_HOST,
            port=self.config.DB_PORT,
            user=self.config.DB_USER,
            password=self.config.DB_PASSWORD,
            database=self.config.DB_NAME,
            charset=self.config.DB_CHARSET,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
        )
        try:
            yield conn
        finally:
            conn.close()

    def fetch_all(self, sql, params=None):
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                return cursor.fetchall()

    def fetch_one(self, sql, params=None):
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                return cursor.fetchone()

    def execute(self, sql, params=None):
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params or ())
            conn.commit()

    def execute_rowcount(self, sql, params=None):
        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                rowcount = cursor.rowcount
            conn.commit()
        return rowcount

    def get_member_by_username(self, username):
        return self.fetch_one(
            """
            SELECT user_id, username, password_hash, register_date, user_type, created_at
            FROM users
            WHERE username = %s
            LIMIT 1
            """,
            (username,),
        )

    def get_member_by_user_id(self, user_id):
        return self.fetch_one(
            """
            SELECT user_id, username, password_hash, register_date, user_type, created_at
            FROM users
            WHERE user_id = %s
            LIMIT 1
            """,
            (user_id,),
        )

    def create_member(self, username, password_hash):
        user_id = f"WEB{uuid4().hex[:12].upper()}"
        self.execute(
            """
            INSERT INTO users (user_id, username, password_hash, register_date, user_type)
            VALUES (%s, %s, %s, %s, 'new')
            """,
            (user_id, username, password_hash, date.today()),
        )
        return self.get_member_by_user_id(user_id)

    def get_member_profile(self, user_id):
        return self.fetch_one(
            """
            SELECT
                u.user_id,
                u.username,
                u.register_date,
                CASE
                    WHEN DATEDIFF(CURDATE(), u.register_date) >= 365 THEN 'old'
                    ELSE 'new'
                END AS user_type,
                COALESCE(a.rating_count, 0) AS rating_count,
                a.avg_rating
            FROM users u
            LEFT JOIN active_users a ON a.user_id = u.user_id
            WHERE u.user_id = %s
            LIMIT 1
            """,
            (user_id,),
        )

    def get_user_history(self, user_id, limit=20):
        return self.fetch_all(
            """
            SELECT h.user_id, h.meal_id, m.meal_name, h.rating, h.review_date, h.review_time,
                   r.review
            FROM user_order_history h
            LEFT JOIN meals m ON m.meal_id = h.meal_id
            LEFT JOIN meal_reviews r ON r.user_id = h.user_id AND r.meal_id = h.meal_id
            WHERE h.user_id = %s
            ORDER BY h.review_time DESC
            LIMIT %s
            """,
            (user_id, limit),
        )

    def user_has_ordered_meal(self, user_id, meal_id):
        row = self.fetch_one(
            """
            SELECT 1 AS exists_flag
            FROM user_order_history
            WHERE user_id = %s AND meal_id = %s
            LIMIT 1
            """,
            (user_id, meal_id),
        )
        return bool(row)

    def get_meal(self, meal_id):
        return self.fetch_one(
            """
            SELECT meal_id, meal_name, category, avg_rating, rating_count, popularity_rank
            FROM meals
            WHERE meal_id = %s
            LIMIT 1
            """,
            (meal_id,),
        )

    def search_meals(self, query, limit=30):
        keyword = f"%{query}%"
        return self.fetch_all(
            """
            SELECT meal_id, meal_name, category, avg_rating, rating_count, popularity_rank
            FROM meals
            WHERE meal_id LIKE %s OR meal_name LIKE %s
            ORDER BY
                CASE WHEN meal_id = %s THEN 0 ELSE 1 END,
                rating_count DESC,
                avg_rating DESC
            LIMIT %s
            """,
            (keyword, keyword, query, limit),
        )

    def get_meal_review_stats(self, meal_id):
        row = self.fetch_one(
            """
            SELECT AVG(rating) AS avg_rating, COUNT(*) AS rating_count
            FROM meal_reviews
            WHERE meal_id = %s
            """,
            (meal_id,),
        )
        if not row:
            return {"avg_rating": None, "rating_count": 0}
        return {
            "avg_rating": row["avg_rating"],
            "rating_count": int(row["rating_count"] or 0),
        }

    def get_meal_reviews(self, meal_id, limit=30):
        return self.fetch_all(
            """
            SELECT user_id, meal_id, rating, review, review_date, review_time
            FROM meal_reviews
            WHERE meal_id = %s
            ORDER BY review_time DESC
            LIMIT %s
            """,
            (meal_id, limit),
        )

    def get_realtime_top_meals(self, limit=10):
        try:
            return self.fetch_all(
                """
                SELECT s.meal_id, m.meal_name, m.category,
                       s.review_count_2s, s.avg_rating_2s, s.hot_score_2s,
                       s.window_start, s.window_end, s.updated_at
                FROM meal_realtime_stats_current s
                LEFT JOIN meals m ON m.meal_id = s.meal_id
                ORDER BY s.hot_score_2s DESC, s.review_count_2s DESC, s.avg_rating_2s DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            return []

    def get_realtime_meal_stats(self, meal_id):
        try:
            return self.fetch_one(
                """
                SELECT meal_id, review_count_2s, avg_rating_2s, hot_score_2s,
                       window_start, window_end, updated_at
                FROM meal_realtime_stats_current
                WHERE meal_id = %s
                LIMIT 1
                """,
                (meal_id,),
            )
        except Exception:
            return None

    def get_latest_reviews(self, limit=10):
        # 来源优先：Flask 用户提交(source=flask_review_form)置顶，回放/历史评论按时间倒序排在其后。
        # (r.source = 'flask_review_form') 为布尔表达式，MySQL 中置顶行得到 1，其余得到 0。
        return self.fetch_all(
            """
            SELECT r.user_id, r.meal_id, m.meal_name, r.rating, r.review,
                   r.review_date, r.review_time, r.source
            FROM meal_reviews r
            LEFT JOIN meals m ON m.meal_id = r.meal_id
            ORDER BY (r.source = 'flask_review_form') DESC, r.review_time DESC, r.id DESC
            LIMIT %s
            """,
            (limit,),
        )

    def get_live_review_count(self, since_minutes=10):
        # 实时计数：最近 N 分钟 Consumer 写入的新评论数。
        # 不区分来源，回放线与用户提交线都计入，用于体现"数据持续进入系统"。
        row = self.fetch_one(
            """
            SELECT COUNT(*) AS live_count
            FROM meal_reviews
            WHERE review_time >= UNIX_TIMESTAMP(NOW() - INTERVAL %s MINUTE)
            """,
            (since_minutes,),
        )
        return int(row["live_count"]) if row else 0

    def publish_review_event(self, event):
        from kafka import KafkaProducer
        import json

        producer = KafkaProducer(
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False
            ).encode("utf-8"),
        )
        try:
            # 超时从 10s 缩到 3s：Kafka 不可达时尽快失败，由路由层 catch 后友好提示，不阻塞页面
            producer.send(self.config.REVIEW_TOPIC, event).get(timeout=3)
            producer.flush()
        finally:
            producer.close()

    def upsert_review_event(self, event):
        review_date = event.get("review_date") or date.today().isoformat()
        review_time = int(event.get("review_time") or datetime.now().timestamp())
        params = (
            event["user_id"],
            event["meal_id"],
            float(event["rating"]),
            event.get("review", ""),
            review_date,
            review_time,
        )
        self.execute(
            """
            INSERT INTO meal_reviews (user_id, meal_id, rating, review, review_date, review_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            params,
        )
        updated = self.execute_rowcount(
            """
            UPDATE user_order_history
            SET rating = %s, review_date = %s, review_time = %s
            WHERE user_id = %s AND meal_id = %s
            """,
            (params[2], params[4], params[5], params[0], params[1]),
        )
        if updated == 0:
            self.execute(
                """
                INSERT INTO user_order_history (user_id, meal_id, rating, review_date, review_time)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (params[0], params[1], params[2], params[4], params[5]),
            )

    def get_rating_summary(self):
        return self.fetch_one("SELECT * FROM rating_summary LIMIT 1") or {}

    def get_rating_distribution(self):
        rows = self.fetch_all(
            """
            SELECT rating, rating_count,
                   rating_count / NULLIF((SELECT SUM(rating_count) FROM rating_distribution), 0) AS ratio
            FROM rating_distribution
            WHERE rating BETWEEN 1 AND 5
            ORDER BY rating
            """
        )
        return rows

    def get_popular_meals(self, limit=10):
        return self.fetch_all(
            """
            SELECT meal_id, rating_count, avg_rating
            FROM popular_meals
            ORDER BY rating_count DESC, avg_rating DESC
            LIMIT %s
            """,
            (limit,),
        )

    def get_active_users(self, limit=10):
        return self.fetch_all(
            """
            SELECT user_id, rating_count, avg_rating,
                   CASE
                       WHEN rating_count >= 50 THEN '高活跃'
                       WHEN rating_count >= 10 THEN '中活跃'
                       ELSE '低活跃'
                   END AS activity_level
            FROM active_users
            ORDER BY rating_count DESC, avg_rating DESC
            LIMIT %s
            """,
            (limit,),
        )

    def get_user_profile(self, user_id):
        return self.fetch_one(
            """
            SELECT user_id, rating_count, avg_rating,
                   CASE
                       WHEN rating_count >= 50 THEN '高活跃'
                       WHEN rating_count >= 10 THEN '中活跃'
                       ELSE '低活跃'
                   END AS activity_level
            FROM active_users
            WHERE user_id = %s
            LIMIT 1
            """,
            (user_id,),
        )

    def get_als_user_recommendations(self, user_id, limit=10):
        return self.fetch_all(
            """
            SELECT user_id, meal_id, user_index, meal_index, rank_no,
                   predicted_rating AS predicted_score
            FROM als_user_recommendations
            WHERE user_id = %s
            ORDER BY rank_no
            LIMIT %s
            """,
            (user_id, limit),
        )

    def get_itemcf_user_recommendations(self, user_id, limit=10):
        return self.fetch_all(
            """
            SELECT user_id, meal_id, user_index, meal_index, rank_no,
                   predicted_score, evidence_count
            FROM itemcf_user_recommendations
            WHERE user_id = %s
            ORDER BY rank_no
            LIMIT %s
            """,
            (user_id, limit),
        )

    def get_als_meal_similarities(self, meal_id, limit=10):
        return self.fetch_all(
            """
            SELECT meal_id, similar_meal_id, meal_index, similar_meal_index, rank_no, similarity
            FROM als_meal_similarities
            WHERE meal_id = %s
            ORDER BY rank_no
            LIMIT %s
            """,
            (meal_id, limit),
        )

    def get_itemcf_meal_similarities(self, meal_id, limit=10):
        return self.fetch_all(
            """
            SELECT meal_id, similar_meal_id, meal_index, similar_meal_index, rank_no,
                   similarity, common_users
            FROM itemcf_meal_similarities
            WHERE meal_id = %s
            ORDER BY rank_no
            LIMIT %s
            """,
            (meal_id, limit),
        )

    def get_model_metrics(self):
        return self.fetch_all(
            """
            SELECT model_type, als_rank, max_iter, reg_param, top_similar_n,
                   min_common_users, test_rows, prediction_rows,
                   prediction_coverage, rmse, mae
            FROM model_metrics
            ORDER BY model_type
            """
        )

    def table_counts(self):
        tables = [
            "users",
            "meals",
            "meal_reviews",
            "user_order_history",
            "rating_summary",
            "rating_distribution",
            "popular_meals",
            "active_users",
            "als_user_recommendations",
            "itemcf_user_recommendations",
            "als_meal_similarities",
            "itemcf_meal_similarities",
            "model_metrics",
            "meal_realtime_window_stats",
            "meal_realtime_stats_current",
        ]
        counts = []
        for table in tables:
            row = self.fetch_one(f"SELECT COUNT(*) AS row_count FROM `{table}`")
            counts.append({"table_name": table, "row_count": row["row_count"]})
        return counts
