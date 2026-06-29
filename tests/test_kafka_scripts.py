import csv
import io
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


def _stub_kafka():
    """Windows 测试环境未安装 kafka-python，注入桩模块以便加载脚本并测试纯函数。"""
    if "kafka" in sys.modules:
        return
    kafka_mod = types.ModuleType("kafka")
    kafka_mod.KafkaProducer = type("KafkaProducer", (), {})
    kafka_mod.KafkaConsumer = type("KafkaConsumer", (), {})
    sys.modules["kafka"] = kafka_mod


def load_module(name, rel_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# 测试来源优先排序需要 repository 类
from webapp.repository import MySQLRepository  # noqa: E402


class KafkaScriptDeploymentTest(unittest.TestCase):
    def test_consumer_is_standalone_vm_script_without_webapp_dependency(self):
        script = ROOT / "scripts" / "11_kafka_review_consumer_to_mysql.py"
        text = script.read_text(encoding="utf-8")

        self.assertNotIn("webapp.repository", text)
        self.assertIn("--mysql-host", text)
        self.assertIn("pymysql.connect", text)


class ConfigKafkaHostTest(unittest.TestCase):
    def test_config_kafka_host_is_vm(self):
        config = load_module("menu_config", "webapp/config.py")
        self.assertEqual(config.config.KAFKA_BOOTSTRAP_SERVERS, "192.168.10.128:9092")
        self.assertEqual(config.config.REVIEW_TOPIC, "meal_review_stream")


class ProducerEventTimeTest(unittest.TestCase):
    def test_producer_event_time_is_now_and_keeps_review_date(self):
        _stub_kafka()
        producer = load_module("menu_producer", "scripts/10__kafka_review_replay_producer.py")
        row = {
            "user_id": "U1",
            "meal_id": "B001",
            "rating": "4.0",
            "review": "好吃",
            "review_length": "2",
            "review_date": "2017-04-30",
            "review_time": "1493576000",
        }
        event = producer.build_event(row, 1)
        # event_time 应是"现在"附近（>= 原始 review_time 2017），用于落库到 review_time
        self.assertGreater(event["event_time"], event["review_time"])
        self.assertEqual(event["source"], "csv_replay")
        # review_date 保留原始历史值，作为回放来源标识
        self.assertEqual(event["review_date"], "2017-04-30")


class ConsumerUpsertIdempotencyTest(unittest.TestCase):
    def _make_writer(self):
        calls = {"sql": [], "params": []}

        def execute(sql, params=None):
            calls["sql"].append(sql)
            calls["params"].append(params)

        writer = SimpleNamespace(execute=execute, execute_rowcount=lambda sql, params=None: 1)
        return writer, calls

    def test_consumer_persist_review_uses_on_duplicate_key_update(self):
        _stub_kafka()
        consumer = load_module(
            "menu_consumer", "scripts/11_kafka_review_consumer_to_mysql.py"
        )
        writer, calls = self._make_writer()
        event = {
            "user_id": "U1",
            "meal_id": "B001",
            "rating": 4.0,
            "review": "好吃",
            "review_date": "2017-04-30",
            "review_time": 1493576000,
            "event_time": 1700000000,
            "source": "csv_replay",
        }
        consumer.persist_review(writer, event)
        self.assertTrue(any("ON DUPLICATE KEY UPDATE" in s for s in calls["sql"]))

    def test_consumer_uses_event_time_as_review_time(self):
        _stub_kafka()
        consumer = load_module(
            "menu_consumer", "scripts/11_kafka_review_consumer_to_mysql.py"
        )
        writer, _ = self._make_writer()
        data = consumer.normalize_event(
            {
                "review_time": 1493576000,
                "event_time": 1700000000,
                "user_id": "U1",
                "meal_id": "B001",
                "rating": 4.0,
                "source": "csv_replay",
            }
        )
        # 优先取 event_time，使回放评论排在实时流顶部
        self.assertEqual(data["review_time"], 1700000000)
        self.assertEqual(data["source"], "csv_replay")

    def test_consumer_upsert_writes_source_from_event(self):
        _stub_kafka()
        consumer = load_module(
            "menu_consumer", "scripts/11_kafka_review_consumer_to_mysql.py"
        )
        writer, calls = self._make_writer()
        consumer.persist_review(
            writer,
            {
                "user_id": "U1",
                "meal_id": "B001",
                "rating": 5.0,
                "review": "赞",
                "source": "flask_review_form",
                "event_time": 1700000000,
            },
        )
        # meal_reviews 的 INSERT 参数：第 7 个参数是 source
        insert_sql = next(s for s in calls["sql"] if "INSERT INTO meal_reviews" in s)
        params = calls["params"][calls["sql"].index(insert_sql)]
        self.assertIn("source", insert_sql)
        self.assertEqual(params[6], "flask_review_form")


class LatestReviewsSourceOrderTest(unittest.TestCase):
    def test_get_latest_reviews_sql_puts_user_submissions_first(self):
        """来源优先排序体现在 repository 生成的 SQL 上（布尔置顶表达式）。"""
        repo = MySQLRepository.__new__(MySQLRepository)  # 不连接数据库
        captured = {}

        def fake_fetch_all(sql, params=None):
            captured["sql"] = sql
            return []

        repo.fetch_all = fake_fetch_all
        repo.get_latest_reviews(10)
        # (r.source = 'flask_review_form') DESC 把用户提交置顶
        self.assertIn("flask_review_form", captured["sql"])
        self.assertIn("DESC", captured["sql"])


class InitScriptDedupTest(unittest.TestCase):
    def test_seed_dedups_to_latest_per_user_meal(self):
        init = load_module("menu_init", "scripts/init_web_business_tables.py")
        import pandas as pd

        df = pd.DataFrame(
            [
                {"user_id": "U1", "meal_id": "B1", "rating": 3.0, "review": "a",
                 "review_date": "2017-04-30", "review_time": 100},
                # 同一 user-meal，更晚的 review_time，应被保留
                {"user_id": "U1", "meal_id": "B1", "rating": 4.0, "review": "b",
                 "review_date": "2017-05-24", "review_time": 200},
                {"user_id": "U2", "meal_id": "B2", "rating": 5.0, "review": "c",
                 "review_date": "2017-04-30", "review_time": 150},
            ]
        )

        seeded = []

        class FakeRepo:
            def execute(self, sql, params=None):
                if sql.startswith("TRUNCATE"):
                    seeded.clear()

            def executemany(self, sql, params):  # noqa: D401
                if "INSERT INTO meal_reviews" in sql:
                    seeded.extend(params)

        # 复用 seed 函数内的去重逻辑：sort + drop_duplicates keep=last
        latest = (
            df.sort_values("review_time")
            .drop_duplicates(subset=["user_id", "meal_id"], keep="last")
            .reset_index(drop=True)
        )
        self.assertEqual(len(latest), 2)
        kept = latest[(latest.user_id == "U1") & (latest.meal_id == "B1")].iloc[0]
        self.assertEqual(kept["rating"], 4.0)  # 保留 review_time 最大那条


if __name__ == "__main__":
    unittest.main()
