import argparse
import json
from datetime import datetime

import pymysql
from pyflink.common import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import (
    AggregateFunction,
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import SlidingProcessingTimeWindows
from pyflink.common.typeinfo import Types


DEFAULT_TOPIC = "meal_review_stream"


def parse_review_event(raw):
    try:
        event = json.loads(raw)
        meal_id = str(event.get("meal_id") or "").strip()
        rating = float(event.get("rating") or 0)
        if not meal_id or rating < 1 or rating > 5:
            return "", 0.0
        return meal_id, rating
    except Exception:
        return "", 0.0


class RatingAggregate(AggregateFunction):
    def create_accumulator(self):
        return 0, 0.0

    def add(self, value, accumulator):
        count, total = accumulator
        return count + 1, total + float(value[1])

    def get_result(self, accumulator):
        count, total = accumulator
        avg_rating = total / count if count else 0.0
        hot_score = count * 5.0 + avg_rating * 2.0
        return count, avg_rating, hot_score

    def merge(self, first, second):
        return first[0] + second[0], first[1] + second[1]


class WindowResult(ProcessWindowFunction):
    def process(self, key, context, elements):
        count, avg_rating, hot_score = next(iter(elements))
        window_start = datetime.fromtimestamp(context.window().start / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        window_end = datetime.fromtimestamp(context.window().end / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        yield {
            "meal_id": key,
            "window_start": window_start,
            "window_end": window_end,
            "review_count": int(count),
            "avg_rating": float(avg_rating),
            "hot_score": float(hot_score),
        }


class MySQLRealtimeMap(MapFunction):
    """
    把窗口统计结果作为副作用写入 MySQL（PyFlink 1.13 的 add_sink 只支持
    Java 实现的 SinkFunction，Python 自定义 sink 无法直接挂载，因此用
    MapFunction 的 open/map/close 生命周期来承载外部写入）。
    """

    def __init__(self, args):
        self.args = args
        self.conn = None

    def open(self, runtime_context):
        self.conn = pymysql.connect(
            host=self.args.mysql_host,
            port=self.args.mysql_port,
            user=self.args.mysql_user,
            password=self.args.mysql_password,
            database=self.args.mysql_db,
            charset=self.args.mysql_charset,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    def map(self, value):
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO meal_realtime_window_stats
                    (meal_id, window_start, window_end, review_count, avg_rating, hot_score)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    review_count = VALUES(review_count),
                    avg_rating = VALUES(avg_rating),
                    hot_score = VALUES(hot_score),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    value["meal_id"],
                    value["window_start"],
                    value["window_end"],
                    value["review_count"],
                    value["avg_rating"],
                    value["hot_score"],
                ),
            )
            cursor.execute(
                """
                INSERT INTO meal_realtime_stats_current
                    (meal_id, review_count_2s, avg_rating_2s, hot_score_2s, window_start, window_end)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    review_count_2s = VALUES(review_count_2s),
                    avg_rating_2s = VALUES(avg_rating_2s),
                    hot_score_2s = VALUES(hot_score_2s),
                    window_start = VALUES(window_start),
                    window_end = VALUES(window_end),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    value["meal_id"],
                    value["review_count"],
                    value["avg_rating"],
                    value["hot_score"],
                    value["window_start"],
                    value["window_end"],
                ),
            )
        self.conn.commit()
        # 返回 JSON 字符串，便于后续 print 观察输出
        return json.dumps(value, ensure_ascii=False)

    def close(self):
        if self.conn:
            self.conn.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute 2-second sliding review windows from Kafka with Flink."
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--group-id", default="meal-review-flink-window-stats")
    parser.add_argument("--window-seconds", type=int, default=2)
    parser.add_argument("--slide-seconds", type=int, default=1)
    parser.add_argument(
        "--kafka-connector-jar",
        default="",
        help=(
            "Optional local flink-connector-kafka jar path. Use a jar matching "
            "the Flink version installed in the VM, for example "
            "/opt/flink/lib/flink-connector-kafka_2.11-1.13.x.jar."
        ),
    )
    parser.add_argument("--mysql-host", default="localhost")
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-user", default="root")
    parser.add_argument("--mysql-password", default="root")
    parser.add_argument("--mysql-db", default="menu_recommendation")
    parser.add_argument("--mysql-charset", default="utf8mb4")
    return parser.parse_args()


def main():
    args = parse_args()
    env = StreamExecutionEnvironment.get_execution_environment()
    if args.kafka_connector_jar:
        jar_path = args.kafka_connector_jar
        if not jar_path.startswith("file://"):
            jar_path = "file://" + jar_path
        env.add_jars(jar_path)
    env.set_parallelism(1)

    kafka_props = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "latest",
    }
    consumer = FlinkKafkaConsumer(args.topic, SimpleStringSchema(), kafka_props)

    stream = env.add_source(consumer)
    (
        stream.map(parse_review_event, output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()]))
        .filter(lambda row: bool(row[0]))
        .key_by(lambda row: row[0])
        .window(
            SlidingProcessingTimeWindows.of(
                Time.seconds(args.window_seconds),
                Time.seconds(args.slide_seconds),
            )
        )
        .aggregate(RatingAggregate(), WindowResult())
        .map(MySQLRealtimeMap(args), output_type=Types.STRING())
        .print()
    )

    env.execute("meal-review-2s-window-stats")


if __name__ == "__main__":
    main()
