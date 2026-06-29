import argparse
import json
from contextlib import contextmanager
from datetime import date, datetime

import pymysql
from kafka import KafkaConsumer


DEFAULT_TOPIC = "meal_review_stream"


class MySQLWriter:
    def __init__(self, args):
        self.host = args.mysql_host
        self.port = args.mysql_port
        self.user = args.mysql_user
        self.password = args.mysql_password
        self.database = args.mysql_db
        self.charset = args.mysql_charset

    @contextmanager
    def connect(self):
        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset=self.charset,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
        )
        try:
            yield conn
        finally:
            conn.close()

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


def normalize_event(event):
    now = datetime.now()
    review = (event.get("review") or "").strip()
    # 落库时间戳优先取 event_time（消息到达系统时刻），保证回放/新评论排在实时流顶部；
    # 回退到 review_time（兼容旧消息），再回退到当前时刻
    review_time = int(event.get("event_time") or event.get("review_time") or now.timestamp())
    return {
        "user_id": str(event.get("user_id") or "").strip(),
        "meal_id": str(event.get("meal_id") or "").strip(),
        "rating": float(event.get("rating") or 0),
        "review": review,
        "review_date": event.get("review_date") or date.today().isoformat(),
        "review_time": review_time,
        "source": str(event.get("source") or "replay").strip() or "replay",
    }


def persist_review(writer, event):
    data = normalize_event(event)
    if not data["user_id"] or not data["meal_id"]:
        raise ValueError("user_id and meal_id are required")
    if data["rating"] < 1 or data["rating"] > 5:
        raise ValueError("rating must be between 1 and 5")

    # meal_reviews 用 ON DUPLICATE KEY UPDATE 幂等写入：同一 (user_id, meal_id)
    # 重复消费/重跑只会更新而不会堆积，这是回放可重跑的前提
    writer.execute(
        """
        INSERT INTO meal_reviews (user_id, meal_id, rating, review, review_date, review_time, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            rating = VALUES(rating),
            review = VALUES(review),
            review_date = VALUES(review_date),
            review_time = VALUES(review_time),
            source = VALUES(source)
        """,
        (
            data["user_id"],
            data["meal_id"],
            data["rating"],
            data["review"],
            data["review_date"],
            data["review_time"],
            data["source"],
        ),
    )

    # user_order_history 已有 UNIQUE(user_id, meal_id)，update→insert 兜底天然幂等
    updated = writer.execute_rowcount(
        """
        UPDATE user_order_history
        SET rating = %s, review_date = %s, review_time = %s
        WHERE user_id = %s AND meal_id = %s
        """,
        (
            data["rating"],
            data["review_date"],
            data["review_time"],
            data["user_id"],
            data["meal_id"],
        ),
    )
    if updated == 0:
        writer.execute(
            """
            INSERT INTO user_order_history (user_id, meal_id, rating, review_date, review_time)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                data["user_id"],
                data["meal_id"],
                data["rating"],
                data["review_date"],
                data["review_time"],
            ),
        )
    return data


def parse_args():
    parser = argparse.ArgumentParser(
        description="Consume review events from Kafka and write them into MySQL."
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--group-id", default="meal-review-mysql-writer")
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--mysql-host", default="localhost")
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-user", default="root")
    parser.add_argument("--mysql-password", default="root")
    parser.add_argument("--mysql-db", default="menu_recommendation")
    parser.add_argument("--mysql-charset", default="utf8mb4")
    return parser.parse_args()


def main():
    args = parse_args()
    writer = MySQLWriter(args)
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(
        f"listening topic={args.topic} bootstrap={args.bootstrap_servers} "
        f"mysql={args.mysql_user}@{args.mysql_host}:{args.mysql_port}/{args.mysql_db}"
    )
    for message in consumer:
        try:
            data = persist_review(writer, message.value)
            print(
                f"inserted review: user={data['user_id']} "
                f"meal={data['meal_id']} rating={data['rating']}"
            )
        except Exception as exc:
            print(f"skip invalid review event: {exc}; raw={message.value}")


if __name__ == "__main__":
    main()
