import argparse
import csv
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "user_meal_rating_cleaned.csv"
DEFAULT_TOPIC = "meal_review_stream"


def open_csv_source(input_path):
    value = str(input_path)
    if value.startswith("hdfs://"):
        hdfs_path = value.removeprefix("hdfs://")
        proc = subprocess.Popen(
            ["hdfs", "dfs", "-cat", hdfs_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        return proc.stdout, proc
    return open(value, "r", encoding="utf-8-sig", newline=""), None


def build_event(row, sequence):
    now = datetime.now()
    review = (row.get("review") or "").strip()
    return {
        "event_id": f"replay-{sequence}-{int(now.timestamp())}",
        "source": "csv_replay",
        "user_id": (row.get("user_id") or "").strip(),
        "meal_id": (row.get("meal_id") or "").strip(),
        "rating": float(row.get("rating") or 0),
        "review": review,
        "review_length": int(row.get("review_length") or len(review)),
        # 原始数据时间，保留作为历史回放来源标识（落库到 review_date 列）
        "review_date": row.get("review_date") or now.date().isoformat(),
        # review_time 保留原始值，供调试/溯源；Consumer 不直接用此值落库
        "review_time": int(row.get("review_time") or now.timestamp()),
        # 落库时间戳：消息到达系统的当前时刻，Consumer 用此值写入 review_time 列，
        # 使回放评论排在实时流顶部，演示"数据持续进入系统"
        "event_time": int(now.timestamp()),
        "created_at": now.isoformat(timespec="seconds"),
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replay cleaned meal reviews into Kafka as a simulated live stream."
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="CSV path or hdfs:///path")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between messages")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N rows; 0 means all rows")
    return parser.parse_args()


def main():
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )

    stream, proc = open_csv_source(args.input)
    count = 0
    try:
        reader = csv.DictReader(stream)
        for row in reader:
            if args.limit and count >= args.limit:
                break
            count += 1
            event = build_event(row, count)
            if not event["user_id"] or not event["meal_id"] or event["rating"] <= 0:
                continue
            producer.send(args.topic, event)
            producer.flush()
            print(
                f"sent review {count}: user={event['user_id']} "
                f"meal={event['meal_id']} rating={event['rating']}"
            )
            time.sleep(args.interval)
    finally:
        producer.close()
        stream.close()
        if proc is not None:
            proc.wait(timeout=5)

    print(f"finished, total sent: {count}")


if __name__ == "__main__":
    main()
