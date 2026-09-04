import sys
import math
import builtins
import h3

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    udf,
    to_timestamp,
    year,
    month,
    dayofweek,
    hour,
    expr,
    count,
    sum as spark_sum,
    round,
    concat_ws,
    sha2
)
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.window import Window


# Paths

SILVER_PATH = "hdfs://localhost:9000/data/staging/rides_geo/"
GOLD_PATH = "hdfs://localhost:9000/data/gold/rides_summary/"


# UDF 1: H3 geohash

@udf(returnType=StringType())
def lat_lng_to_h3(lat, lng):
    if lat is None or lng is None:
        return None

    try:
        return h3.latlng_to_cell(
            float(lat),
            float(lng),
            8
        )
    except Exception:
        return None


# UDF 2: Haversine distance

@udf(returnType=DoubleType())
def haversine_distance(lat1, lon1, lat2, lon2):

    if None in (lat1, lon1, lat2, lon2):
        return None

    try:
        R = 3958.8  # Earth radius in miles

        phi1 = math.radians(float(lat1))
        phi2 = math.radians(float(lat2))

        delta_phi = math.radians(
            float(lat2) - float(lat1)
        )

        delta_lambda = math.radians(
            float(lon2) - float(lon1)
        )

        a = (
            math.sin(delta_phi / 2.0) ** 2
            + math.cos(phi1)
            * math.cos(phi2)
            * math.sin(delta_lambda / 2.0) ** 2
        )

        # Protect against tiny floating-point errors.
        a = min(1.0, max(0.0, a))

        c = 2 * math.atan2(
            math.sqrt(a),
            math.sqrt(1 - a)
        )

        return builtins.round(R * c, 2)

    except Exception:
        return None


def main():

    # 1. Input batch path

    if len(sys.argv) < 2:
        raise ValueError(
            "Usage: spark-submit script.py <input_batch_path>"
        )

    input_file_path = sys.argv[1]

    print(
        f"--> Processing incoming batch: {input_file_path}",
        flush=True
    )


    # 2. Spark session

    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_Incremental_Processor")
        .config("spark.driver.memory", "1536m")
        .config("spark.network.timeout", "800s")
        .getOrCreate()
    )

    df_silver_batch = None
    df_existing_gold = None

    try:

        # SILVER

        # 3. Read CURRENT batch only

        df_bronze = spark.read.parquet(
            input_file_path
        )

        print(
            "--> Current batch loaded.",
            flush=True
        )


        # 4. Clean and transform CURRENT batch

        df_silver_batch = (
            df_bronze

            # Basic quality filters
            .filter(
                (col("Start_Lat").isNotNull()) &
                (col("Start_Lon").isNotNull()) &
                (col("End_Lat").isNotNull()) &
                (col("End_Lon").isNotNull()) &
                (col("Start_Lat") != 0) &
                (col("Start_Lon") != 0) &
                (col("End_Lat") != 0) &
                (col("End_Lon") != 0) &
                (col("Passenger_Count") > 0) &
                (col("Trip_Distance") > 0)
            )

            .withColumn(
                "start_time",
                to_timestamp(
                    col("Trip_Pickup_DateTime")
                )
            )
            .withColumn(
                "end_time",
                to_timestamp(
                    col("Trip_Dropoff_DateTime")
                )
            )

            # Make sure timestamp conversion succeeded
            .filter(
                col("start_time").isNotNull() &
                col("end_time").isNotNull()
            )

            # Trip duration
            .withColumn(
                "trip_duration_sec",
                col("end_time").cast("long")
                - col("start_time").cast("long")
            )

            # Date dimensions
            .withColumn(
                "pickup_year",
                year(col("start_time"))
            )
            .withColumn(
                "pickup_month",
                month(col("start_time"))
            )
            .withColumn(
                "day_of_week",
                dayofweek(col("start_time"))
            )
            .withColumn(
                "is_weekend",
                expr(
                    "CASE "
                    "WHEN day_of_week IN (1, 7) "
                    "THEN 1 ELSE 0 END"
                )
            )

            # H3 spatial features
            .withColumn(
                "start_geo_hash",
                lat_lng_to_h3(
                    col("Start_Lat"),
                    col("Start_Lon")
                )
            )
            .withColumn(
                "end_geo_hash",
                lat_lng_to_h3(
                    col("End_Lat"),
                    col("End_Lon")
                )
            )

            # Haversine distance
            .withColumn(
                "haversine_miles",
                haversine_distance(
                    col("Start_Lat"),
                    col("Start_Lon"),
                    col("End_Lat"),
                    col("End_Lon")
                )
            )

            # Deterministic ID
            .withColumn(
                "trip_id",
                sha2(
                    concat_ws(
                        "||",
                        col("start_time"),
                        col("end_time"),
                        col("Start_Lat"),
                        col("Start_Lon"),
                        col("End_Lat"),
                        col("End_Lon"),
                        col("Passenger_Count"),
                        col("Trip_Distance"),
                        col("Fare_Amt")
                    ),
                    256
                )
            )


            .select(
                "trip_id",
                "start_time",
                "end_time",
                "Start_Lat",
                "Start_Lon",
                "End_Lat",
                "End_Lon",
                "Passenger_Count",
                "Trip_Distance",
                "Fare_Amt",
                "trip_duration_sec",
                "day_of_week",
                "is_weekend",
                "start_geo_hash",
                "end_geo_hash",
                "haversine_miles",
                "pickup_year",
                "pickup_month"
            )

            # ------------------------------------------------
            # The same transformed batch is used for both
            # Silver output and Gold aggregation.
            # ------------------------------------------------
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )


        # 5. APPEND current batch to Silver

        (
            df_silver_batch.write
            .mode("append")
            .partitionBy(
                "pickup_year",
                "pickup_month"
            )
            .option(
                "compression",
                "snappy"
            )
            .parquet(
                SILVER_PATH
            )
        )

        print(
            "--> Current batch appended to Silver.",
            flush=True
        )


        # GOLD - INCREMENTAL AGGREGATION

        # 6. Aggregate CURRENT BATCH only

        df_batch_aggregate = (
            df_silver_batch

            .filter(
                col("start_geo_hash").isNotNull()
            )

            .withColumn(
                "pickup_hour",
                hour(col("start_time"))
            )

            .groupBy(
                "start_geo_hash",
                "pickup_hour"
            )

            .agg(
                count("*").alias(
                    "batch_trip_count"
                ),

                spark_sum(
                    "Fare_Amt"
                ).alias(
                    "batch_total_fare"
                ),

                spark_sum(
                    "Trip_Distance"
                ).alias(
                    "batch_total_recorded_distance"
                ),

                spark_sum(
                    "haversine_miles"
                ).alias(
                    "batch_total_haversine_distance"
                )
            )
        )

        print(
            "--> Current batch aggregated.",
            flush=True
        )


        # 7. Check whether GOLD contains actual Parquet data

        hadoop_conf = (
            spark.sparkContext
            ._jsc
            .hadoopConfiguration()
        )

        path_class = (
            spark.sparkContext
            ._gateway
            .jvm
            .org.apache.hadoop.fs.Path
        )

        gold_path = path_class(
            GOLD_PATH
        )

        fs = gold_path.getFileSystem(
            hadoop_conf
        )

        gold_has_data = False

        if fs.exists(gold_path):

            for file_status in fs.listStatus(
                gold_path
            ):

                file_path = file_status.getPath()
                file_name = file_path.getName()

                if (
                    file_status.isFile()
                    and file_name.endswith(".parquet")
                ):
                    gold_has_data = True
                    break


        # 8. Read EXISTING GOLD state

        if gold_has_data:

            df_existing_gold = (
                spark.read
                .parquet(
                    GOLD_PATH
                )
                .select(
                    "start_geo_hash",
                    "pickup_hour",
                    "total_trips",
                    "total_fare",
                    "total_recorded_distance",
                    "total_haversine_distance"
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
            )

            # Force existing Gold state to be read and cached
            # before the Gold path is overwritten.
            existing_gold_count = (
                df_existing_gold.count()
            )

            print(
                f"--> Existing Gold state loaded "
                f"({existing_gold_count} rows).",
                flush=True
            )

        else:

            print(
                "--> No existing Gold data found. "
                "Creating initial Gold state.",
                flush=True
            )

            df_existing_gold = None


        # 9. Convert batch aggregation into Gold state schema

        df_batch_state = (
            df_batch_aggregate
            .select(
                col("start_geo_hash"),
                col("pickup_hour"),

                col("batch_trip_count")
                .alias("total_trips"),

                col("batch_total_fare")
                .alias("total_fare"),

                col("batch_total_recorded_distance")
                .alias(
                    "total_recorded_distance"
                ),

                col("batch_total_haversine_distance")
                .alias(
                    "total_haversine_distance"
                )
            )
        )


        # 10. MERGE existing Gold state + current batch state

        if df_existing_gold is None:

            df_gold_state = df_batch_state

        else:

            df_gold_state = (
                df_existing_gold

                .unionByName(
                    df_batch_state
                )

                .groupBy(
                    "start_geo_hash",
                    "pickup_hour"
                )

                .agg(
                    spark_sum(
                        "total_trips"
                    ).alias(
                        "total_trips"
                    ),

                    spark_sum(
                        "total_fare"
                    ).alias(
                        "total_fare"
                    ),

                    spark_sum(
                        "total_recorded_distance"
                    ).alias(
                        "total_recorded_distance"
                    ),

                    spark_sum(
                        "total_haversine_distance"
                    ).alias(
                        "total_haversine_distance"
                    )
                )
            )


        print(
            "--> Existing Gold + current batch merged.",
            flush=True
        )


        # 11. Calculate GLOBAL derived metrics

        df_gold_final = (
            df_gold_state

            .withColumn(
                "avg_fare",
                round(
                    col("total_fare")
                    / col("total_trips"),
                    2
                )
            )

            .withColumn(
                "avg_recorded_distance",
                round(
                    col("total_recorded_distance")
                    / col("total_trips"),
                    2
                )
            )

            .withColumn(
                "avg_haversine_distance",
                round(
                    col("total_haversine_distance")
                    / col("total_trips"),
                    2
                )
            )
        )


        # 12. Calculate running average fare

        window_spec = (
            Window
            .partitionBy(
                "start_geo_hash"
            )
            .orderBy(
                "pickup_hour"
            )
            .rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow
            )
        )

        df_gold_final = (
            df_gold_final

            .withColumn(
                "cumulative_fare",
                spark_sum(
                    "total_fare"
                ).over(
                    window_spec
                )
            )

            .withColumn(
                "cumulative_trips",
                spark_sum(
                    "total_trips"
                ).over(
                    window_spec
                )
            )

            .withColumn(
                "running_avg_fare",
                round(
                    col("cumulative_fare")
                    / col("cumulative_trips"),
                    2
                )
            )

            .drop(
                "cumulative_fare",
                "cumulative_trips"
            )
        )


        # 13. OVERWRITE GOLD with updated global state

        (
            df_gold_final
            .write
            .mode("overwrite")
            .option(
                "compression",
                "snappy"
            )
            .parquet(
                GOLD_PATH
            )
        )

        print(
            "--> Global Gold state updated successfully.",
            flush=True
        )


    finally:

        if df_existing_gold is not None:
            df_existing_gold.unpersist()

        if df_silver_batch is not None:
            df_silver_batch.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
