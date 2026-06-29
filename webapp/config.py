import os


class Config:
    DB_HOST = os.getenv("MENU_DB_HOST", "192.168.10.128")
    DB_PORT = int(os.getenv("MENU_DB_PORT", "3306"))
    DB_USER = os.getenv("MENU_DB_USER", "root")
    DB_PASSWORD = os.getenv("MENU_DB_PASSWORD", "root")
    DB_NAME = os.getenv("MENU_DB_NAME", "menu_recommendation")
    DB_CHARSET = os.getenv("MENU_DB_CHARSET", "utf8mb4")
    SECRET_KEY = os.getenv("SECRET_KEY", "menu-recommendation-dev")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("MENU_KAFKA_BOOTSTRAP_SERVERS", "192.168.10.128:9092")
    REVIEW_TOPIC = os.getenv("MENU_REVIEW_TOPIC", "meal_review_stream")


config = Config()
