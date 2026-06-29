try:
    from webapp.repository import MySQLRepository
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from webapp.repository import MySQLRepository


def create_realtime_window_tables(repo):
    statements = [
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


def main():
    repo = MySQLRepository()
    create_realtime_window_tables(repo)
    for table in ["meal_realtime_window_stats", "meal_realtime_stats_current"]:
        row = repo.fetch_one(f"SELECT COUNT(*) AS row_count FROM {table}")
        print(f"{table}: {row['row_count']}")


if __name__ == "__main__":
    main()
