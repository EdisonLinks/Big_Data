from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, coalesce, explode, lit, row_number
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


INPUT_PATH = "hdfs:///Data/processed/ratings_for_als"
MODEL_OUTPUT_PATH = "hdfs:///Data/model/als_model"
RECOMMEND_OUTPUT_PATH = "hdfs:///Data/output/user_recommendations"
METRICS_OUTPUT_PATH = "hdfs:///Data/output/als_metrics"

SEED = 2026
TOP_N = 10
RAW_RECOMMEND_N = 50


def evaluate_predictions(evaluator, predictions):
    count = predictions.count()
    if count == 0:
        return None, 0
    return float(evaluator.evaluate(predictions)), count


spark = (
    SparkSession.builder
    .appName("TrainALSModelOptimized")
    .getOrCreate()
)

ratings = spark.read.parquet(INPUT_PATH)

als_data = (
    ratings
    .select(
        col("user_index").cast("int").alias("user_index"),
        col("meal_index").cast("int").alias("meal_index"),
        col("rating").cast("float").alias("rating"),
    )
    .dropna(subset=["user_index", "meal_index", "rating"])
    .cache()
)

total_rows = als_data.count()
total_users = als_data.select("user_index").distinct().count()
total_meals = als_data.select("meal_index").distinct().count()

print("===== ALS Training Data =====")
als_data.printSchema()
print("rows:", total_rows)
print("users:", total_users)
print("meals:", total_meals)

train_data, test_data = als_data.randomSplit([0.8, 0.2], seed=SEED)
train_data = train_data.cache()
test_data = test_data.cache()

train_count = train_data.count()
test_count = test_data.count()

print("train rows:", train_count)
print("test rows:", test_count)

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction",
)

metric_rows = []

global_mean = train_data.agg(avg("rating").alias("mean_rating")).first()["mean_rating"]
global_mean_predictions = test_data.withColumn(
    "prediction",
    lit(float(global_mean)).cast(FloatType()),
)
global_rmse, global_pred_count = evaluate_predictions(evaluator, global_mean_predictions)

metric_rows.append((
    "baseline_global_mean",
    None,
    None,
    None,
    global_rmse,
    test_count,
    global_pred_count,
    1.0 if test_count else None,
))

user_mean = train_data.groupBy("user_index").agg(avg("rating").alias("user_mean"))
user_mean_predictions = (
    test_data
    .join(user_mean, on="user_index", how="left")
    .withColumn(
        "prediction",
        coalesce(col("user_mean"), lit(float(global_mean))).cast(FloatType()),
    )
)
user_rmse, user_pred_count = evaluate_predictions(evaluator, user_mean_predictions)

metric_rows.append((
    "baseline_user_mean",
    None,
    None,
    None,
    user_rmse,
    test_count,
    user_pred_count,
    1.0 if test_count else None,
))

item_mean = train_data.groupBy("meal_index").agg(avg("rating").alias("item_mean"))
item_mean_predictions = (
    test_data
    .join(item_mean, on="meal_index", how="left")
    .withColumn(
        "prediction",
        coalesce(col("item_mean"), lit(float(global_mean))).cast(FloatType()),
    )
)
item_rmse, item_pred_count = evaluate_predictions(evaluator, item_mean_predictions)

metric_rows.append((
    "baseline_item_mean",
    None,
    None,
    None,
    item_rmse,
    test_count,
    item_pred_count,
    1.0 if test_count else None,
))

param_candidates = [
    {"rank": 8, "maxIter": 10, "regParam": 0.08},
    {"rank": 8, "maxIter": 10, "regParam": 0.10},
    {"rank": 8, "maxIter": 10, "regParam": 0.15},
    {"rank": 10, "maxIter": 10, "regParam": 0.08},
    {"rank": 10, "maxIter": 10, "regParam": 0.10},
    {"rank": 10, "maxIter": 10, "regParam": 0.15},
    {"rank": 12, "maxIter": 10, "regParam": 0.08},
    {"rank": 12, "maxIter": 10, "regParam": 0.10},
    {"rank": 12, "maxIter": 10, "regParam": 0.15},
    {"rank": 12, "maxIter": 15, "regParam": 0.10},
    {"rank": 16, "maxIter": 10, "regParam": 0.10},
    {"rank": 16, "maxIter": 10, "regParam": 0.15},
]

best_model = None
best_rmse = None
best_params = None

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
        seed=SEED,
    )

    model = als.fit(train_data)
    predictions = model.transform(test_data)
    rmse, prediction_count = evaluate_predictions(evaluator, predictions)
    coverage = float(prediction_count) / float(test_count) if test_count else None

    print("RMSE:", rmse)
    print("prediction rows:", prediction_count)
    print("prediction coverage:", coverage)

    metric_rows.append((
        "als",
        params["rank"],
        params["maxIter"],
        params["regParam"],
        rmse,
        test_count,
        prediction_count,
        coverage,
    ))

    if rmse is not None and (best_rmse is None or rmse < best_rmse):
        best_rmse = rmse
        best_model = model
        best_params = params

if best_model is None:
    raise RuntimeError("No ALS model was trained successfully.")

print("===== Best ALS Model =====")
print("best_params:", best_params)
print("best_rmse:", best_rmse)

metrics_schema = StructType([
    StructField("model_type", StringType(), False),
    StructField("rank", IntegerType(), True),
    StructField("maxIter", IntegerType(), True),
    StructField("regParam", DoubleType(), True),
    StructField("rmse", DoubleType(), True),
    StructField("test_rows", IntegerType(), True),
    StructField("prediction_rows", IntegerType(), True),
    StructField("prediction_coverage", DoubleType(), True),
])

metrics_df = spark.createDataFrame(metric_rows, metrics_schema)
metrics_df.orderBy(col("model_type"), col("rmse").asc_nulls_last()).show(100, truncate=False)
metrics_df.write.mode("overwrite").option("header", "true").csv(METRICS_OUTPUT_PATH)

best_model.write().overwrite().save(MODEL_OUTPUT_PATH)

# Generate more raw recommendations first, then remove meals the user already rated.
raw_recommendations = best_model.recommendForAllUsers(RAW_RECOMMEND_N)

recommendations = (
    raw_recommendations
    .select(
        col("user_index"),
        explode(col("recommendations")).alias("rec"),
    )
    .select(
        col("user_index"),
        col("rec.meal_index").alias("meal_index"),
        col("rec.rating").alias("predicted_rating"),
    )
)

rated_pairs = als_data.select("user_index", "meal_index").distinct()

unrated_recommendations = recommendations.join(
    rated_pairs,
    on=["user_index", "meal_index"],
    how="left_anti",
)

user_mapping = ratings.select(
    col("user_index").cast("int").alias("user_index"),
    col("user_id"),
).distinct()

meal_mapping = ratings.select(
    col("meal_index").cast("int").alias("meal_index"),
    col("meal_id"),
).distinct()

result = (
    unrated_recommendations
    .join(user_mapping, on="user_index", how="left")
    .join(meal_mapping, on="meal_index", how="left")
)

window = Window.partitionBy("user_index").orderBy(col("predicted_rating").desc())

top10_result = (
    result
    .withColumn("rank_no", row_number().over(window))
    .filter(col("rank_no") <= TOP_N)
    .select(
        "user_id",
        "meal_id",
        "user_index",
        "meal_index",
        "rank_no",
        "predicted_rating",
    )
)

top10_result.write.mode("overwrite").parquet(RECOMMEND_OUTPUT_PATH)

print("===== Recommendation Output =====")
print("recommendation rows:", top10_result.count())
print("recommended users:", top10_result.select("user_index").distinct().count())
top10_result.show(20, truncate=False)

als_data.unpersist()
train_data.unpersist()
test_data.unpersist()

spark.stop()
