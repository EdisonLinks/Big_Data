from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from werkzeug.security import generate_password_hash

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "user_meal_rating_cleaned.csv"

try:
    from webapp.repository import MySQLRepository
except ImportError:
    import sys

    sys.path.insert(0, str(ROOT))
    from webapp.repository import MySQLRepository


def execute_many(repo, sql, rows, batch_size=1000):
    with repo.connect() as conn:
        with conn.cursor() as cursor:
            for start in range(0, len(rows), batch_size):
                cursor.executemany(sql, rows[start : start + batch_size])
        conn.commit()


def create_tables(repo):
    statements = [
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR(64) PRIMARY KEY,
            username VARCHAR(80) NOT NULL UNIQUE,
            password_hash VARCHAR(255) NOT NULL,
            register_date DATE NOT NULL,
            user_type VARCHAR(16) NOT NULL DEFAULT 'new',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS meals (
            meal_id VARCHAR(64) PRIMARY KEY,
            meal_name VARCHAR(120) NOT NULL,
            category VARCHAR(40),
            avg_rating DOUBLE,
            rating_count BIGINT,
            popularity_rank INT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS meal_reviews (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(64) NOT NULL,
            meal_id VARCHAR(64) NOT NULL,
            rating DOUBLE,
            review TEXT,
            review_date DATE,
            review_time BIGINT,
            source VARCHAR(32) NOT NULL DEFAULT 'replay',
            UNIQUE KEY uk_user_meal_review (user_id, meal_id),
            INDEX idx_meal_reviews_meal (meal_id),
            INDEX idx_meal_reviews_user (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS user_order_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(64) NOT NULL,
            meal_id VARCHAR(64) NOT NULL,
            rating DOUBLE,
            review_date DATE,
            review_time BIGINT,
            UNIQUE KEY uk_user_meal_history (user_id, meal_id),
            INDEX idx_order_history_user (user_id),
            INDEX idx_order_history_meal (meal_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS meal_realtime_window_stats (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            meal_id VARCHAR(64) NOT NULL,
            window_start DATETIME NOT NULL,
            window_end DATETIME NOT NULL,
            review_count BIGINT NOT NULL,
            avg_rating DOUBLE,
            hot_score DOUBLE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_meal_window (meal_id, window_start, window_end),
            INDEX idx_realtime_window_end (window_end),
            INDEX idx_realtime_hot_score (hot_score)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS meal_realtime_stats_current (
            meal_id VARCHAR(64) PRIMARY KEY,
            review_count_2s BIGINT NOT NULL,
            avg_rating_2s DOUBLE,
            hot_score_2s DOUBLE,
            window_start DATETIME NOT NULL,
            window_end DATETIME NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_current_hot_score (hot_score_2s),
            INDEX idx_current_window_end (window_end)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]
    for sql in statements:
        repo.execute(sql)


def migrate_meal_reviews_schema(repo):
    """兼容已建表：补 source 列与 UNIQUE(user_id, meal_id)。

    MySQL 对含重复数据的表加 UNIQUE 会失败，因此先删除同 (user_id, meal_id)
    的旧行（保留每组最大 id），再补约束。CREATE TABLE IF NOT EXISTS 已覆盖全新库，
    本函数只处理库已存在旧表的情况，是幂等的。
    """
    columns = repo.fetch_all(
        """
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'meal_reviews'
        """,
        (repo.config.DB_NAME,),
    )
    column_names = {row["COLUMN_NAME"] for row in columns}

    # 补 source 列
    if "source" not in column_names:
        repo.execute(
            "ALTER TABLE meal_reviews ADD COLUMN source VARCHAR(32) NOT NULL DEFAULT 'replay'"
        )

    # 补 UNIQUE(user_id, meal_id)：先去重再加约束
    indexes = repo.fetch_all(
        """
        SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'meal_reviews'
        """,
        (repo.config.DB_NAME,),
    )
    index_names = {row["INDEX_NAME"] for row in indexes}
    if "uk_user_meal_review" not in index_names:
        repo.execute(
            """
            DELETE r1 FROM meal_reviews r1
            JOIN meal_reviews r2
              ON r1.user_id = r2.user_id AND r1.meal_id = r2.meal_id
              AND r1.id < r2.id
            """
        )
        repo.execute(
            "ALTER TABLE meal_reviews ADD UNIQUE KEY uk_user_meal_review (user_id, meal_id)"
        )


def seed_users(repo, df):
    active_user_ids = df.groupby("user_id").size().sort_values(ascending=False).head(300).index.tolist()
    repo.execute("DELETE FROM users WHERE username IN ('demo_old', 'demo_new')")
    rows = []
    password = generate_password_hash("123456")
    for idx, user_id in enumerate(active_user_ids, 1):
        username = "demo_old" if idx == 1 else f"user_{idx:03d}"
        rows.append(
            (
                user_id,
                username,
                password,
                date.today() - timedelta(days=500 + idx),
                "old",
            )
        )

    rows.extend(
        [
            ("demo_new", "demo_new", password, date.today(), "new"),
        ]
    )

    execute_many(
        repo,
        """
        INSERT INTO users (user_id, username, password_hash, register_date, user_type)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            username = VALUES(username),
            password_hash = VALUES(password_hash),
            register_date = VALUES(register_date),
            user_type = VALUES(user_type)
        """,
        rows,
    )


def seed_meals(repo, df):
    stats = (
        df.groupby("meal_id")
        .agg(rating_count=("rating", "size"), avg_rating=("rating", "mean"))
        .sort_values(["rating_count", "avg_rating"], ascending=[False, False])
        .reset_index()
    )
    categories = ["家常菜", "川湘风味", "轻食简餐", "面饭套餐", "小吃甜品", "健康餐"]
    rows = []
    for idx, row in stats.iterrows():
        rows.append(
            (
                row["meal_id"],
                f"菜品 {row['meal_id']}",
                categories[idx % len(categories)],
                float(row["avg_rating"]),
                int(row["rating_count"]),
                idx + 1,
            )
        )

    repo.execute("TRUNCATE TABLE meals")
    execute_many(
        repo,
        """
        INSERT INTO meals (meal_id, meal_name, category, avg_rating, rating_count, popularity_rank)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        rows,
    )


def seed_reviews_and_history(repo, df):
    # 同一 (user_id, meal_id) 保留 review_time 最大的一条（最近一次评价），
    # 与 meal_reviews 的 UNIQUE(user_id, meal_id) 约束保持一致。
    latest = (
        df.sort_values("review_time")
        .drop_duplicates(subset=["user_id", "meal_id"], keep="last")
        .reset_index(drop=True)
    )

    review_rows = [
        (
            row.user_id,
            row.meal_id,
            float(row.rating),
            str(row.review) if pd.notna(row.review) else "",
            str(row.review_date),
            int(row.review_time),
        )
        for row in latest.itertuples(index=False)
    ]
    history_rows = [
        (
            row.user_id,
            row.meal_id,
            float(row.rating),
            str(row.review_date),
            int(row.review_time),
        )
        for row in latest.itertuples(index=False)
    ]

    repo.execute("TRUNCATE TABLE meal_reviews")
    repo.execute("TRUNCATE TABLE user_order_history")
    execute_many(
        repo,
        """
        INSERT INTO meal_reviews (user_id, meal_id, rating, review, review_date, review_time, source)
        VALUES (%s, %s, %s, %s, %s, %s, 'replay')
        """,
        review_rows,
    )
    execute_many(
        repo,
        """
        INSERT INTO user_order_history (user_id, meal_id, rating, review_date, review_time)
        VALUES (%s, %s, %s, %s, %s)
        """,
        history_rows,
    )


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(CSV_PATH)

    repo = MySQLRepository()
    df = pd.read_csv(CSV_PATH)
    create_tables(repo)
    migrate_meal_reviews_schema(repo)
    seed_users(repo, df)
    seed_meals(repo, df)
    seed_reviews_and_history(repo, df)

    for table in [
        "users",
        "meals",
        "meal_reviews",
        "user_order_history",
        "meal_realtime_window_stats",
        "meal_realtime_stats_current",
    ]:
        row = repo.fetch_one(f"SELECT COUNT(*) AS row_count FROM {table}")
        print(f"{table}: {row['row_count']}")
    print("demo accounts: demo_old / 123456, demo_new / 123456")


if __name__ == "__main__":
    main()
