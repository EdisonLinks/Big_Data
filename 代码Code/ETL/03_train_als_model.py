from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, row_number
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("TrainALSModel") \
    .getOrCreate()

input_path = "hdfs:///Data/processed/ratings_for_als"
model_output_path = "hdfs:///Data/model/als_model"
recommend_output_path = "hdfs:///Data/output/user_recommendations"
metrics_output_path = "hdfs:///Data/output/als_metrics"

ratings = spark.read.parquet(input_path)

als_data = ratings.select(
    col("user_index").cast("int"),
    col("meal_index").cast("int"),
    col("rating").cast("float")
)

print("===== ALS Training Data =====")
als_data.printSchema()
print("rows:", als_data.count())
print("users:", als_data.select("user_index").distinct().count())
print("meals:", als_data.select("meal_index").distinct().count())

train_data, test_data = als_data.randomSplit([0.8, 0.2], seed=2026)

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

param_candidates = [
    {"rank": 8, "maxIter": 10, "regParam": 0.05},
    {"rank": 8, "maxIter": 10, "regParam": 0.10},
    {"rank": 10, "maxIter": 10, "regParam": 0.05},
    {"rank": 10, "maxIter": 10, "regParam": 0.10},
    {"rank": 12, "maxIter": 10, "regParam": 0.10},
]

best_model = None
best_rmse = None
best_params = None
metrics = []

for params in param_candidates:
    print("===== Training ALS Params =====")
    print(params)

    als = ALS(
        userCol="user_index",
        itemCol="meal_index",
        ratingCol="rating",
        rank=params["rank"],
        maxIter=params["maxIter"],
        regParam=params["regParam"],
        coldStartStrategy="drop",
        nonnegative=True,
        seed=2026
    )

    model = als.fit(train_data)
    predictions = model.transform(test_data)
    rmse = evaluator.evaluate(predictions)

    print("RMSE:", rmse)

    metrics.append((
        params["rank"],
        params["maxIter"],
        params["regParam"],
        float(rmse)
    ))

    if best_rmse is None or rmse < best_rmse:
        best_rmse = rmse
        best_model = model
        best_params = params

print("===== Best ALS Model =====")
print("best_params:", best_params)
print("best_rmse:", best_rmse)

metrics_df = spark.createDataFrame(
    metrics,
    ["rank", "maxIter", "regParam", "rmse"]
)

metrics_df.write.mode("overwrite").option("header", "true").csv(metrics_output_path)

best_model.write().overwrite().save(model_output_path)

# 生成推荐结果。
# 先多推荐一些，再过滤掉用户已经评价过的菜品，最后每个用户保留 Top 10。
raw_recommendations = best_model.recommendForAllUsers(30)

recommendations = raw_recommendations \
    .select(
        col("user_index"),
        explode(col("recommendations")).alias("rec")
    ) \
    .select(
        col("user_index"),
        col("rec.meal_index").alias("meal_index"),
        col("rec.rating").alias("predicted_rating")
    )

rated_pairs = als_data.select("user_index", "meal_index").distinct()

unrated_recommendations = recommendations.join(
    rated_pairs,
    on=["user_index", "meal_index"],
    how="left_anti"
)

user_mapping = ratings.select("user_index", "user_id").distinct()
meal_mapping = ratings.select("meal_index", "meal_id").distinct()

result = unrated_recommendations \
    .join(user_mapping, on="user_index", how="left") \
    .join(meal_mapping, on="meal_index", how="left")

window = Window.partitionBy("user_index").orderBy(col("predicted_rating").desc())

top10_result = result \
    .withColumn("rank_no", row_number().over(window)) \
    .filter(col("rank_no") <= 10) \
    .select(
        "user_id",
        "meal_id",
        "user_index",
        "meal_index",
        "rank_no",
        "predicted_rating"
    )

top10_result.write.mode("overwrite").parquet(recommend_output_path)

print("===== Recommendation Output =====")
print("recommendation rows:", top10_result.count())
top10_result.show(20, truncate=False)

spark.stop()