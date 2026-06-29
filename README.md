# 个性化菜品推荐系统

> Big_Data 项目集中的大数据处理与推荐系统实践项目。

这是一个面向课程设计的大数据个性化推荐项目，围绕“用户-菜品-评分/评论”数据构建离线推荐、实时点评流处理和 Web 展示闭环。系统使用 Spark 在 HDFS 上完成数据检查、特征准备、ALS 协同过滤、ItemCF 相似度计算；使用 Kafka 承接用户点评事件；使用 Flink 计算实时滑动窗口热度；使用 MySQL 存储业务数据、离线推荐结果和实时统计结果；使用 Flask 提供登录、推荐、搜索、详情、看板和可视化页面。

## 技术栈

| 层级 | 技术 |
| --- | --- |
| Web 应用 | Python, Flask, Jinja2, HTML, CSS, JavaScript |
| 数据访问 | PyMySQL, MySQL 8.x |
| 离线计算 | Apache Spark, PySpark, Spark MLlib ALS |
| 推荐算法 | ALS 矩阵分解, ItemCF 基于物品的协同过滤, 热门菜品冷启动 |
| 流式处理 | Kafka, kafka-python, Apache Flink / PyFlink |
| 数据存储 | HDFS, Parquet, CSV, MySQL |
| 可视化 | ECharts, Flask API JSON |
| 测试 | unittest / pytest |

## 项目结构

```text
.
├── webapp/                         # Flask Web 应用
│   ├── app.py                      # 路由、登录注册、推荐页面、API、点评提交
│   ├── config.py                   # MySQL / Kafka / Flask 配置
│   ├── repository.py               # MySQL 查询、Kafka 点评事件发布
│   ├── services.py                 # 推荐策略编排、冷启动、看板数据组织
│   ├── static/                     # CSS 和前端刷新/图表脚本
│   └── templates/                  # Jinja2 页面模板
├── 代码Code/ETL/                   # Spark 离线处理与推荐模型脚本
│   ├── 01_spark_data_check.py
│   ├── 02_prepare_als_data.py
│   ├── 03_train_als_model.py
│   ├── 08_als_item_similarity_recommendation.py
│   └── 09_itemcf_recommendation.py
├── scripts/                        # MySQL 初始化、Kafka 回放、Flink 实时统计
│   ├── init_web_business_tables.py
│   ├── init_realtime_window_tables.py
│   ├── 10__kafka_review_replay_producer.py
│   ├── 11_kafka_review_consumer_to_mysql.py
│   └── 12_flink_review_window_stats.py
├── tests/                          # 服务层、路由、Kafka 脚本测试
├── run_webapp_5001.py              # 以 5001 端口启动 Web 应用
└── user_meal_rating_cleaned.csv    # 本地演示数据文件，默认不上传 GitHub
```

## 核心业务逻辑
<img width="870" height="735" alt="微信图片_20260629111512_1382_94" src="https://github.com/user-attachments/assets/bae55514-7e65-4f91-a25c-dbdaa757d2ad" />

### 1. 离线推荐链路

1. `01_spark_data_check.py` 从 `hdfs:///Data/user_meal_rating_cleaned.csv` 读取用户菜品评分数据，检查行数、用户数、菜品数、评分分布、重复评分、活跃用户和热门菜品。
2. `02_prepare_als_data.py` 对同一用户和菜品保留最新评分，使用 `StringIndexer` 将 `user_id` 和 `meal_id` 编码为 ALS 可训练的数值索引，并输出到 `hdfs:///Data/processed/ratings_for_als`。
3. `03_train_als_model.py` 使用 Spark MLlib ALS 训练多个参数组合，按 RMSE 选择最优模型，生成每个用户 Top 10 推荐结果，并保存模型指标。
4. `08_als_item_similarity_recommendation.py` 读取 ALS item factors，通过余弦相似度生成每个菜品的相似菜品列表。
5. `09_itemcf_recommendation.py` 使用 ItemCF 计算菜品共现相似度，并基于用户历史评分生成辅助推荐，同时输出 RMSE、MAE、覆盖率等指标。

### 2. 实时点评链路

1. Web 端用户在历史点餐页面提交评分和评论。
2. `webapp.repository.MySQLRepository.publish_review_event()` 将点评封装为 JSON 事件并写入 Kafka topic `meal_review_stream`。
3. `scripts/11_kafka_review_consumer_to_mysql.py` 消费 Kafka 点评事件，幂等写入 `meal_reviews` 和 `user_order_history`。
4. `scripts/12_flink_review_window_stats.py` 通过 PyFlink 消费同一 topic，以菜品为 key 计算 2 秒窗口内的评分次数、平均评分和热度分，并写入：
   - `meal_realtime_window_stats`
   - `meal_realtime_stats_current`
5. Web 首页、菜品详情页和数据看板读取实时统计表，展示实时热门菜品和窗口评分变化。

### 3. Web 推荐逻辑

`webapp.services.RecommendationService` 统一封装推荐策略：

- 新注册用户或无历史行为用户：使用热门高评分菜品做冷启动推荐。
- 有历史行为用户：同时展示 ALS 主推荐和 ItemCF 辅助推荐。
- 离线推荐缺失时：自动回退到热门菜品，保证页面始终有可展示内容。
- 菜品详情页：同时展示 ALS 相似菜品、ItemCF 相似菜品、历史评论和 Flink 实时窗口统计。
- 看板和可视化页：聚合评分概况、评分分布、热门菜品、活跃用户、模型指标、最新评论和实时热门菜品。
**效果演示图**
<img width="909" height="464" alt="微信图片_20260629111639_1386_94" src="https://github.com/user-attachments/assets/c063773a-2af1-448e-8ecc-bd5f3e841710" />
<img width="909" height="262" alt="微信图片_20260629111603_1383_94" src="https://github.com/user-attachments/assets/632e8a49-9198-43be-abf9-59be85e995d5" />
<img width="909" height="430" alt="微信图片_20260629111639_1384_94" src="https://github.com/user-attachments/assets/471ca405-653a-4436-ad7b-878a73dbed89" />


## MySQL 主要表

| 表名 | 作用 |
| --- | --- |
| `users` | Web 登录用户、注册日期、用户类型 |
| `meals` | 菜品基础信息、平均评分、热度排名 |
| `meal_reviews` | 用户对菜品的评分和评论 |
| `user_order_history` | 用户历史点餐和评分行为 |
| `rating_summary` | 全局评分统计汇总 |
| `rating_distribution` | 1 到 5 星评分分布 |
| `popular_meals` | 热门高评分菜品 |
| `active_users` | 活跃用户画像 |
| `als_user_recommendations` | ALS 用户推荐结果 |
| `itemcf_user_recommendations` | ItemCF 用户推荐结果 |
| `als_meal_similarities` | ALS 菜品相似度 |
| `itemcf_meal_similarities` | ItemCF 菜品相似度 |
| `model_metrics` | ALS / ItemCF 模型评估指标 |
| `meal_realtime_window_stats` | Flink 窗口统计历史 |
| `meal_realtime_stats_current` | 每个菜品最新实时热度 |

## 环境变量

Web 应用默认读取以下环境变量，未设置时使用 `webapp/config.py` 中的开发默认值。

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `MENU_DB_HOST` | `192.168.10.128` | MySQL 主机 |
| `MENU_DB_PORT` | `3306` | MySQL 端口 |
| `MENU_DB_USER` | `root` | MySQL 用户 |
| `MENU_DB_PASSWORD` | `root` | MySQL 密码 |
| `MENU_DB_NAME` | `menu_recommendation` | MySQL 数据库 |
| `MENU_DB_CHARSET` | `utf8mb4` | MySQL 字符集 |
| `SECRET_KEY` | `menu-recommendation-dev` | Flask Session 密钥 |
| `MENU_KAFKA_BOOTSTRAP_SERVERS` | `192.168.10.128:9092` | Kafka 地址 |
| `MENU_REVIEW_TOPIC` | `meal_review_stream` | 点评事件 topic |

## 本地运行

### 1. 安装依赖

```bash
python -m venv .venv
pip install -r requirements.txt
```

### 2. 初始化业务表和演示数据

确保 MySQL 可连接，并且根目录存在 `user_meal_rating_cleaned.csv`。

```bash
python scripts/init_web_business_tables.py
python scripts/init_realtime_window_tables.py
```

初始化后可使用演示账号：

```text
demo_old / 123456
demo_new / 123456
```

### 3. 启动 Web 应用

```bash
python run_webapp_5001.py
```

浏览器访问：

```text
http://127.0.0.1:5001
```

### 4. 启动实时链路

先启动 Zookeeper、Kafka、Hadoop/HDFS，然后按需运行：

```bash
python scripts/10__kafka_review_replay_producer.py --bootstrap-servers localhost:9092 --interval 2 --limit 100
python scripts/11_kafka_review_consumer_to_mysql.py --bootstrap-servers localhost:9092 --mysql-host localhost
python scripts/12_flink_review_window_stats.py --bootstrap-servers localhost:9092 --mysql-host localhost
```

### 5. 运行测试

```bash
python -m pytest
```

## 关键页面与接口

| 路径 | 功能 |
| --- | --- |
| `/` | 登录入口 |
| `/register` | 用户注册 |
| `/home` | 用户首页、个性化推荐、冷启动推荐、实时热门菜品 |
| `/history` | 历史点餐与点评提交 |
| `/search` | 菜品搜索 |
| `/meal/<meal_id>` | 菜品详情、评论、相似菜品、实时统计 |
| `/dashboard` | 推荐系统数据看板 |
| `/visualization` | ECharts 数据可视化 |
| `/models` | 模型评估指标 |
| `/api/dashboard` | 看板 JSON 数据 |
| `/api/visualization` | 可视化 JSON 数据 |
| `/api/recommend/<user_id>` | 指定用户推荐结果 |
| `/api/similar/<meal_id>` | 指定菜品相似推荐 |

