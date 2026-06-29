import csv
import json
import time
from kafka import KafkaProducer

INPUT = "/home/hadoop/menu-recommendation/data/user_meal_rating_cleaned.csv"
TOPIC = "meal_rating_stream"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

count = 0

with open(INPUT, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        producer.send(TOPIC, row)
        count += 1

        if count % 1000 == 0:
            print(f"sent {count} rows")

        time.sleep(0.001)

producer.flush()
producer.close()

print(f"finished, total sent: {count}")
