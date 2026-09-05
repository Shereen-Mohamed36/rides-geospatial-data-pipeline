#!/usr/bin/env python3
"""
NYC Taxi ML Pipeline
Spark 3.1.2 compatible training, evaluation, and prediction pipeline for trip-level Parquet data.
"""

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from functools import reduce
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark import SparkConf
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


DEFAULT_INPUT_PATH = "hdfs://localhost:9000/data/staging/rides_geo/"
DEFAULT_MODEL_ROOT = "hdfs://localhost:9000/data/ml/models"
DEFAULT_PREDICTION_ROOT = "hdfs://localhost:9000/data/ml/predictions"

# give spark enough mem but leave room for OS/HDFS
DEFAULT_DRIVER_MEMORY = "4g"

DEFAULT_MIN_ROWS = 100
DEFAULT_TRAIN_FRACTION = 0.70
DEFAULT_VALIDATION_FRACTION = 0.15

DEFAULT_GBT_MAX_DEPTH = 8
DEFAULT_GBT_MAX_ITER = 100
DEFAULT_GBT_MAX_BINS = 64
DEFAULT_GBT_SUBSAMPLING_RATE = 0.8

DEFAULT_RF_NUM_TREES = 80
DEFAULT_RF_MAX_DEPTH = 10
DEFAULT_RF_MAX_BINS = 64
DEFAULT_RF_SUBSAMPLING_RATE = 0.8

DEFAULT_LR_MAX_ITER = 100
DEFAULT_LR_REG_PARAM = 0.0
DEFAULT_LR_ELASTIC_NET_PARAM = 0.0

TARGET_DURATION = "duration"
TARGET_FARE = "fare"
VALID_TARGETS = {TARGET_DURATION, TARGET_FARE}

ALGO_GBT = "gbt"
ALGO_RF = "rf"
ALGO_LR = "lr"
VALID_ALGOS = {ALGO_GBT, ALGO_RF, ALGO_LR}

DURATION_LABEL = "trip_duration_sec"
FARE_LABEL = "Fare_Amt"

BASE_NUMERIC_FEATURES = [
    "Start_Lat",
    "Start_Lon",
    "End_Lat",
    "End_Lon",
    "haversine_miles",
    "Passenger_Count",
    "pickup_hour",
    "day_of_week",
    "is_weekend",
    "pickup_month",
    "is_rush_hour",
    "pickup_dist_jfk",
    "dropoff_dist_jfk",
    "pickup_dist_lga",
    "dropoff_dist_lga",
    "pickup_dist_ewr",
    "dropoff_dist_ewr",
]

AIRPORTS = {
    "jfk": (40.6413, -73.7781),
    "lga": (40.7769, -73.8740),
    "ewr": (40.6895, -74.1745),
}

REQUIRED_COLUMNS = {
    "Start_Lat",
    "Start_Lon",
    "End_Lat",
    "End_Lon",
    "Passenger_Count",
    "haversine_miles",
    "start_time",
    "pickup_month",
    DURATION_LABEL,
    FARE_LABEL,
}

LEAKY_OR_POST_TRIP_COLUMNS = {
    "Trip_Distance",
    FARE_LABEL,
    DURATION_LABEL,
    "end_time",
    "Tip_Amt",
    "Total_Amt",
    "Tolls_Amt",
    "surcharge",
    "mta_tax",
    "speed",
}


def fail(message: str) -> None:
    raise ValueError(message)


def require_columns(df: DataFrame, required: Iterable[str]) -> None:
    actual = set(df.columns)
    missing = sorted(set(required) - actual)
    if missing:
        fail(f"Missing required columns: {', '.join(missing)}")


def validate_finite_number(value: float, name: str) -> float:
    if not math.isfinite(value):
        fail(f"{name} must be finite, got {value}")
    return value


def generate_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def hdfs_path_join(base: str, child: str) -> str:
    return base.rstrip("/") + "/" + child.strip("/")


def create_spark_session(app_name: str) -> SparkSession:
    conf = (
        SparkConf()
        .setAppName(app_name)
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.shuffle.partitions", "8")
        .set("spark.sql.files.maxPartitionBytes", "64m")
    )
    return SparkSession.builder.config(conf=conf).getOrCreate()


def safe_count(df: DataFrame) -> int:
    return int(df.count())


def safe_mean(df: DataFrame, column: str) -> float:
    row = df.select(F.avg(F.col(column)).alias("mean")).first()
    if row is None or row["mean"] is None:
        fail(f"Could not compute mean for {column}")
    return validate_finite_number(float(row["mean"]), f"training mean for {column}")


def approx_median_absolute_error(
    df_with_errors: DataFrame, absolute_error_col: str = "absolute_error"
) -> Optional[float]:
    values = (
        df_with_errors.select(F.col(absolute_error_col).cast("double"))
        .where(F.col(absolute_error_col).isNotNull())
        .approxQuantile(absolute_error_col, [0.5], 0.01)
    )
    return validate_finite_number(float(values[0]), "median absolute error") if values else None


def write_json(spark: SparkSession, path: str, payload: Dict) -> None:
    text = json.dumps(payload, sort_keys=True, indent=2)
    spark.createDataFrame([(text,)], ["json"]).coalesce(1).write.mode("overwrite").text(path)


def ensure_start_time_type(df: DataFrame) -> DataFrame:
    dtype = dict(df.dtypes).get("start_time")
    if dtype == "timestamp":
        return df

    if dtype == "string":
        parsed = F.to_timestamp(F.col("start_time"))
        invalid = df.where(F.col("start_time").isNotNull() & parsed.isNull()).limit(1).count()
        if invalid:
            fail("start_time contains strings that can't be parsed to timestamp.")
        return df.withColumn("start_time", parsed)

    fail(f"start_time must be timestamp or string, got {dtype!r}")


def haversine_expr(lat_col: str, lon_col: str, ref_lat: float, ref_lon: float):
    lat = F.radians(F.col(lat_col).cast("double"))
    lon = F.radians(F.col(lon_col).cast("double"))
    ref_lat_rad = math.radians(ref_lat)
    ref_lon_rad = math.radians(ref_lon)

    dlat = lat - F.lit(ref_lat_rad)
    dlon = lon - F.lit(ref_lon_rad)

    a = (
        F.pow(F.sin(dlat / F.lit(2.0)), F.lit(2.0))
        + F.cos(lat)
        * F.lit(math.cos(ref_lat_rad))
        * F.pow(F.sin(dlon / F.lit(2.0)), F.lit(2.0))
    )
    safe_a = F.least(F.lit(1.0), F.greatest(F.lit(0.0), a))
    earth_radius_miles = 3958.7613
    return F.lit(2.0 * earth_radius_miles) * F.asin(F.sqrt(safe_a))


def clean_and_engineer_features(df: DataFrame, target: str, require_label: bool = True) -> DataFrame:
    required = REQUIRED_COLUMNS.copy()
    if not require_label:
        required.discard(DURATION_LABEL)
        required.discard(FARE_LABEL)
        
    require_columns(df, required)
    df = ensure_start_time_type(df)
    label_col = label_column(target)

    cast_columns = [
        "Start_Lat", "Start_Lon", "End_Lat", "End_Lon",
        "Passenger_Count", "haversine_miles", "pickup_month"
    ]
    if require_label:
        cast_columns.append(label_col)

    for column in cast_columns:
        df = df.withColumn(column, F.col(column).cast("double"))

    validity = (
        F.col("Start_Lat").between(-90.0, 90.0)
        & F.col("Start_Lon").between(-180.0, 180.0)
        & F.col("End_Lat").between(-90.0, 90.0)
        & F.col("End_Lon").between(-180.0, 180.0)
        & (F.col("Passenger_Count") > 0)
        & (F.col("haversine_miles") >= 0.0)
        & F.col("start_time").isNotNull()
    )

    if require_label:
        if target == TARGET_DURATION:
            validity = validity & (F.col(label_col) > 0.0)
        else:
            validity = validity & F.col(label_col).isNotNull() & (F.col(label_col) >= 0.0)

    df = df.where(validity)

    df = (
        df.withColumn("pickup_hour", F.hour(F.col("start_time")).cast("double"))
        .withColumn("pickup_month", F.month(F.col("start_time")).cast("double"))
        .withColumn("day_of_week", (F.dayofweek(F.col("start_time")) - F.lit(1)).cast("double"))
        .withColumn("is_weekend", F.when(F.dayofweek(F.col("start_time")).isin([1, 7]), 1.0).otherwise(0.0))
        .withColumn(
            "is_rush_hour",
            F.when(F.col("pickup_hour").isin([7.0, 8.0, 9.0, 10.0, 16.0, 17.0, 18.0, 19.0]), 1.0).otherwise(0.0),
        )
        .withColumn("pickup_year", F.year(F.col("start_time")).cast("int"))
    )

    for airport, (lat, lon) in AIRPORTS.items():
        df = df.withColumn(f"pickup_dist_{airport}", haversine_expr("Start_Lat", "Start_Lon", lat, lon)) \
               .withColumn(f"dropoff_dist_{airport}", haversine_expr("End_Lat", "End_Lon", lat, lon))

    finite_conditions = [
        F.col(column).isNotNull() & ~F.isnan(F.col(column).cast("double"))
        for column in BASE_NUMERIC_FEATURES
    ]
    return df.where(reduce(lambda left, right: left & right, finite_conditions))


def feature_columns() -> List[str]:
    return list(BASE_NUMERIC_FEATURES)


def label_column(target: str) -> str:
    if target == TARGET_DURATION:
        return DURATION_LABEL
    if target == TARGET_FARE:
        return FARE_LABEL
    fail(f"Bad target: {target}")


def chronological_split(
    df: DataFrame, train_fraction: float, validation_fraction: float
) -> Tuple[DataFrame, DataFrame, DataFrame, Dict]:
    # deterministic split ordered by pickup time
    test_fraction = 1.0 - train_fraction - validation_fraction
    total_rows = safe_count(df)
    
    if total_rows < 3:
        fail(f"Need at least 3 rows to split, got {total_rows}")

    order_columns = [F.col("start_time").asc()]
    if "trip_id" in df.columns:
        order_columns.append(F.col("trip_id").asc_nulls_last())
    else:
        order_columns.extend([
            F.col("Start_Lat").asc(),
            F.col("Start_Lon").asc(),
            F.col("End_Lat").asc(),
            F.col("End_Lon").asc(),
            F.col("Passenger_Count").asc(),
        ])

    window = Window.orderBy(*order_columns)
    numbered = df.withColumn("__chronological_row_number", F.row_number().over(window))

    train_end = int(math.floor(total_rows * train_fraction))
    val_end = int(math.floor(total_rows * (train_fraction + validation_fraction)))
    train_end = max(1, min(train_end, total_rows - 2))
    val_end = max(train_end + 1, min(val_end, total_rows - 1))

    train = numbered.where(F.col("__chronological_row_number") <= train_end).drop("__chronological_row_number")
    validation = numbered.where(
        (F.col("__chronological_row_number") > train_end) & (F.col("__chronological_row_number") <= val_end)
    ).drop("__chronological_row_number")
    test = numbered.where(F.col("__chronological_row_number") > val_end).drop("__chronological_row_number")

    train_max = train.select(F.max("start_time").alias("ts")).first()["ts"]
    val_min = validation.select(F.min("start_time").alias("ts")).first()["ts"]
    val_max = validation.select(F.max("start_time").alias("ts")).first()["ts"]
    test_min = test.select(F.min("start_time").alias("ts")).first()["ts"]

    # sanity check ordering
    if not (train_max <= val_min <= val_max <= test_min):
        fail("Chronological ordering check failed.")

    metadata = {
        "strategy": "chronological_row_fraction",
        "train_fraction_requested": train_fraction,
        "validation_fraction_requested": validation_fraction,
        "test_fraction_requested": test_fraction,
        "total_valid_rows": total_rows,
        "train_boundary_row": train_end,
        "validation_boundary_row": val_end,
        "train_max_start_time": train_max.isoformat() if hasattr(train_max, "isoformat") else str(train_max),
        "validation_min_start_time": val_min.isoformat() if hasattr(val_min, "isoformat") else str(val_min),
        "validation_max_start_time": val_max.isoformat() if hasattr(val_max, "isoformat") else str(val_max),
        "test_min_start_time": test_min.isoformat() if hasattr(test_min, "isoformat") else str(test_min),
    }
    return train, validation, test, metadata


def validate_split_sizes(train: DataFrame, validation: DataFrame, test: DataFrame, min_rows: int) -> Dict[str, int]:
    counts = {
        "train": safe_count(train),
        "validation": safe_count(validation),
        "test": safe_count(test),
    }
    if any(c < min_rows for c in counts.values()):
        fail(f"Splits don't meet min_rows={min_rows}. Counts: {counts}")
    return counts


def build_pipeline(algorithm: str, label_col: str, args: argparse.Namespace) -> Pipeline:
    assembler = VectorAssembler(inputCols=feature_columns(), outputCol="features", handleInvalid="skip")

    if algorithm == ALGO_GBT:
        estimator = GBTRegressor(
            labelCol=label_col, featuresCol="features", predictionCol="prediction",
            maxDepth=args.gbt_max_depth, maxIter=args.gbt_max_iter,
            maxBins=args.gbt_max_bins, subsamplingRate=args.gbt_subsampling_rate, seed=args.seed
        )
    elif algorithm == ALGO_RF:
        estimator = RandomForestRegressor(
            labelCol=label_col, featuresCol="features", predictionCol="prediction",
            numTrees=args.rf_num_trees, maxDepth=args.rf_max_depth,
            maxBins=args.rf_max_bins, subsamplingRate=args.rf_subsampling_rate, seed=args.seed
        )
    elif algorithm == ALGO_LR:
        estimator = LinearRegression(
            labelCol=label_col, featuresCol="features", predictionCol="prediction",
            maxIter=args.lr_max_iter, regParam=args.lr_reg_param, elasticNetParam=args.lr_elastic_net_param
        )
    else:
        fail(f"Bad algo: {algorithm}")

    return Pipeline(stages=[assembler, estimator])


def regression_metrics(
    scored: DataFrame, label_col: str, prediction_col: str = "prediction", fare_mape_threshold: Optional[float] = None
) -> Dict[str, Optional[float]]:
    valid = scored.where(F.col(label_col).isNotNull() & F.col(prediction_col).isNotNull())
    if safe_count(valid) == 0:
        fail(f"No valid rows to eval {label_col}")

    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol=prediction_col)
    
    metrics = {
        "rmse": validate_finite_number(float(evaluator.evaluate(valid, {evaluator.metricName: "rmse"})), "RMSE"),
        "mae": validate_finite_number(float(evaluator.evaluate(valid, {evaluator.metricName: "mae"})), "MAE"),
        "r2": validate_finite_number(float(evaluator.evaluate(valid, {evaluator.metricName: "r2"})), "R2"),
    }

    errors = valid.withColumn("absolute_error", F.abs(F.col(label_col) - F.col(prediction_col)))
    metrics["median_absolute_error"] = approx_median_absolute_error(errors)

    if fare_mape_threshold is not None:
        fare_rows = valid.where(F.col(label_col) > F.lit(fare_mape_threshold))
        if safe_count(fare_rows) == 0:
            metrics["mape_percent"] = None
        else:
            row = fare_rows.select(
                F.avg(F.abs((F.col(label_col) - F.col(prediction_col)) / F.col(label_col)) * F.lit(100.0)).alias("mape_percent")
            ).first()
            mape = None if row is None else row["mape_percent"]
            metrics["mape_percent"] = None if mape is None else validate_finite_number(float(mape), "MAPE")

    return metrics


def baseline_metrics(test_df: DataFrame, label_col: str, train_mean: float, fare_mape_threshold: Optional[float] = None) -> Dict:
    scored = test_df.withColumn("prediction", F.lit(float(train_mean)))
    return regression_metrics(scored, label_col, "prediction", fare_mape_threshold)


def print_metrics(title: str, metrics: Dict[str, Optional[float]]) -> None:
    print(f"\n{title}")
    for k, v in metrics.items():
        print(f"  {k}: {'null' if v is None else f'{v:.6f}'}")


def train(args: argparse.Namespace, spark: SparkSession) -> int:
    target = args.target
    label_col = label_column(target)

    print("\n--- Training Pipeline ---")
    print(f"Target: {target} ({label_col}), Algo: {args.algorithm}")
    
    raw = spark.read.parquet(args.input_path)
    prepared = clean_and_engineer_features(raw, target=target, require_label=True)
    print(f"Valid rows: {safe_count(prepared)}")

    train_df, validation_df, test_df, split_metadata = chronological_split(prepared, args.train_fraction, args.validation_fraction)
    split_counts = validate_split_sizes(train_df, validation_df, test_df, args.min_rows)

    print(f"Split counts: {split_counts}")
    train_mean = safe_mean(train_df, label_col)
    
    pipeline = build_pipeline(args.algorithm, label_col, args)
    print("Fitting model...")
    model = pipeline.fit(train_df)

    print("Scoring splits...")
    validation_scored = model.transform(validation_df)
    test_scored = model.transform(test_df)

    mape_threshold = 1.0 if target == TARGET_FARE else None
    validation_metrics = regression_metrics(validation_scored, label_col, "prediction", mape_threshold)
    test_metrics = regression_metrics(test_scored, label_col, "prediction", mape_threshold)
    test_baseline = baseline_metrics(test_df, label_col, train_mean, mape_threshold)

    print_metrics("Val metrics", validation_metrics)
    print_metrics("Test metrics", test_metrics)

    run_id = args.run_id or generate_run_id()
    model_path = hdfs_path_join(hdfs_path_join(args.model_root, target), run_id)

    print(f"\nSaving model to {model_path}")
    if args.overwrite:
        model.write().overwrite().save(model_path)
    else:
        model.write().save(model_path)

    metadata = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "target_alias": target,
        "label_column": label_col,
        "algorithm": args.algorithm,
        "input_path": args.input_path,
        "model_path": model_path,
        "feature_columns": feature_columns(),
        "excluded_post_trip_columns": sorted(LEAKY_OR_POST_TRIP_COLUMNS),
        "split": {**split_metadata, "row_counts": split_counts},
        "train_mean": train_mean,
        "validation_metrics": validation_metrics,
        "test_metrics": test_metrics,
        "test_mean_baseline_metrics": test_baseline,
        "hyperparameters": {
            "driver_memory_default": DEFAULT_DRIVER_MEMORY,
            "gbt_max_depth": args.gbt_max_depth,
            "gbt_max_iter": args.gbt_max_iter,
            "gbt_max_bins": args.gbt_max_bins,
            "gbt_subsampling_rate": args.gbt_subsampling_rate,
            "rf_num_trees": args.rf_num_trees,
            "rf_max_depth": args.rf_max_depth,
            "rf_max_bins": args.rf_max_bins,
            "rf_subsampling_rate": args.rf_subsampling_rate,
            "lr_max_iter": args.lr_max_iter,
            "lr_reg_param": args.lr_reg_param,
            "lr_elastic_net_param": args.lr_elastic_net_param,
            "seed": args.seed,
        },
    }

    metadata_path = hdfs_path_join(model_path, "run_metadata")
    write_json(spark, metadata_path, metadata)
    
    print(f"Done. Run ID: {run_id}")
    return 0


def evaluate(args: argparse.Namespace, spark: SparkSession) -> int:
    target = args.target
    label_col = label_column(target)

    if not args.model_path:
        fail("--model-path is required for eval")

    print("\n--- Evaluation ---")
    raw = spark.read.parquet(args.input_path)
    prepared = clean_and_engineer_features(raw, target=target, require_label=True)
    
    total_rows = safe_count(prepared)
    if total_rows < args.min_rows:
        fail(f"Eval data has {total_rows} rows, need {args.min_rows}")

    model = PipelineModel.load(args.model_path)
    scored = model.transform(prepared)
    metrics = regression_metrics(scored, label_col, "prediction", 1.0 if target == TARGET_FARE else None)

    print(f"Rows evaluated: {total_rows}")
    print_metrics("Metrics", metrics)
    return 0


def predict(args: argparse.Namespace, spark: SparkSession) -> int:
    target = args.target
    label_col = label_column(target)

    if not args.model_path or not args.output_path:
        fail("predict mode needs both --model-path and --output-path")

    print("\n--- Prediction ---")
    raw = spark.read.parquet(args.input_path)
    prepared = clean_and_engineer_features(raw, target=target, require_label=False)
    
    input_count = safe_count(prepared)
    if input_count == 0:
        fail("No valid rows after cleaning")

    model = PipelineModel.load(args.model_path)
    scored = model.transform(prepared)

    selected = [
        c for c in [
            "trip_id", "start_time", "end_time", "Passenger_Count",
            "Start_Lat", "Start_Lon", "End_Lat", "End_Lon", "haversine_miles",
            label_col, "pickup_year", "pickup_month", "pickup_hour",
            "day_of_week", "is_weekend", "is_rush_hour"
        ] if c in scored.columns
    ] + ["prediction"]

    output = scored.select(*selected)
    writer = output.write.mode("overwrite" if args.overwrite else "errorifexists")
    if args.output_partition_by_month:
        writer = writer.partitionBy("pickup_year", "pickup_month")
    
    writer.parquet(args.output_path)
    print(f"Scored {input_count} rows.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NYC Taxi ML pipeline")
    parser.add_argument("--mode", required=True, choices=["train", "evaluate", "predict"])
    parser.add_argument("--target", required=True, choices=sorted(VALID_TARGETS))
    parser.add_argument("--algorithm", default=ALGO_GBT, choices=sorted(VALID_ALGOS))
    parser.add_argument("--input-path", default=DEFAULT_INPUT_PATH)
    parser.add_argument("--model-root", default=DEFAULT_MODEL_ROOT)
    parser.add_argument("--model-path", default=None)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--min-rows", type=int, default=DEFAULT_MIN_ROWS)
    parser.add_argument("--train-fraction", type=float, default=DEFAULT_TRAIN_FRACTION)
    parser.add_argument("--validation-fraction", type=float, default=DEFAULT_VALIDATION_FRACTION)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--gbt-max-depth", type=int, default=DEFAULT_GBT_MAX_DEPTH)
    parser.add_argument("--gbt-max-iter", type=int, default=DEFAULT_GBT_MAX_ITER)
    parser.add_argument("--gbt-max-bins", type=int, default=DEFAULT_GBT_MAX_BINS)
    parser.add_argument("--gbt-subsampling-rate", type=float, default=DEFAULT_GBT_SUBSAMPLING_RATE)
    parser.add_argument("--rf-num-trees", type=int, default=DEFAULT_RF_NUM_TREES)
    parser.add_argument("--rf-max-depth", type=int, default=DEFAULT_RF_MAX_DEPTH)
    parser.add_argument("--rf-max-bins", type=int, default=DEFAULT_RF_MAX_BINS)
    parser.add_argument("--rf-subsampling-rate", type=float, default=DEFAULT_RF_SUBSAMPLING_RATE)
    parser.add_argument("--lr-max-iter", type=int, default=DEFAULT_LR_MAX_ITER)
    parser.add_argument("--lr-reg-param", type=float, default=DEFAULT_LR_REG_PARAM)
    parser.add_argument("--lr-elastic-net-param", type=float, default=DEFAULT_LR_ELASTIC_NET_PARAM)
    parser.add_argument("--output-partition-by-month", action="store_true")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.min_rows < 1: fail("min-rows must be >= 1")
    if not 0.0 < args.train_fraction < 1.0: fail("bad train-fraction")
    if not 0.0 < args.validation_fraction < 1.0: fail("bad val-fraction")
    if args.train_fraction + args.validation_fraction >= 1.0: fail("fractions sum to >= 1.0")

    if args.run_id and (not args.run_id.strip() or any(c in args.run_id for c in "/\\\0")):
        fail("Invalid run-id")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        validate_args(args)
        spark = create_spark_session("NYC-Taxi-ML-Pipeline")
        try:
            if args.mode == "train": return train(args, spark)
            if args.mode == "evaluate": return evaluate(args, spark)
            if args.mode == "predict": return predict(args, spark)
        finally:
            spark.stop()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())