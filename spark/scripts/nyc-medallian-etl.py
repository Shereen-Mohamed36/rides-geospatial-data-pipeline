import sys
import math
import h3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_timestamp, year, month, dayofweek, hour, expr, count, avg, round, monotonically_increasing_id
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.window import Window

# UDF 1: H3 Geohashing (v4 API)
@udf(returnType=StringType())
def lat_lng_to_h3(lat, lng):
    if lat is None or lng is None:
        return None
    try:
        return h3.latlng_to_cell(float(lat), float(lng), 8)
    except:
        return None

# UDF 2: Haversine Distance Calculation
@udf(returnType=DoubleType())
def haversine_distance(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    try:
        R = 3958.8  # Earth radius in miles
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        
        a = math.sin(delta_phi / 2.0)**2 + \
            math.cos(phi1) * math.cos(phi2) * \
            math.sin(delta_lambda / 2.0)**2
            
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return round(R * c, 2)
    except:
        return None

def main():
    # 1. Capture batch filepath passed from NiFi command argument
    if len(sys.argv) > 1:
        input_file_path = sys.argv[1]
    else:
        input_file_path = "hdfs://localhost:9000/rides_project/raw/rides/*.parquet"

    print(f"--> Processing incoming batch: {input_file_path}")

    # 2. Spark Session Initialization
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Batch_Processor") \
        .config("spark.driver.memory", "1536m") \
        .config("spark.network.timeout", "800s") \
        .getOrCreate()

    # 3. Bronze Layer - Read Batch Data
    df_bronze = spark.read.parquet(input_file_path)

    # 4. Silver Layer - Preprocessing, Geohashing & Required Table Schema
    df_silver_prepared = df_bronze.filter(
        (col("Start_Lat") != 0) & (col("Start_Lon") != 0) &
        (col("End_Lat") != 0) & (col("End_Lon") != 0) &
        (col("Start_Lat").isNotNull()) & (col("Start_Lon").isNotNull()) &
        (col("Passenger_Count") > 0) & (col("Trip_Distance") > 0)
    ).withColumn("trip_id", monotonically_increasing_id()) \
     .withColumn("start_time", to_timestamp(col("Trip_Pickup_DateTime"))) \
     .withColumn("end_time", to_timestamp(col("Trip_Dropoff_DateTime"))) \
     .withColumn("trip_duration_sec", col("end_time").cast("long") - col("start_time").cast("long")) \
     .withColumn("pickup_year", year(col("start_time"))) \
     .withColumn("pickup_month", month(col("start_time"))) \
     .withColumn("day_of_week", dayofweek(col("start_time"))) \
     .withColumn("is_weekend", expr("CASE WHEN day_of_week IN (1, 7) THEN 1 ELSE 0 END")) \
     .withColumn("start_geo_hash", lat_lng_to_h3(col("Start_Lat"), col("Start_Lon"))) \
     .withColumn("end_geo_hash", lat_lng_to_h3(col("End_Lat"), col("End_Lon"))) \
     .withColumn("haversine_miles", haversine_distance(col("Start_Lat"), col("Start_Lon"), col("End_Lat"), col("End_Lon")))

    # Append into Silver Layer in HDFS
    df_silver_prepared.write \
        .mode("append") \
        .partitionBy("pickup_year", "pickup_month") \
        .option("compression", "snappy") \
        .parquet("hdfs://localhost:9000/data/staging/rides_geo/")

    # 5. Gold Layer - Spatial & Time Aggregations (using start_geo_hash)
    df_gold_summary = df_silver_prepared.filter(col("start_geo_hash").isNotNull()) \
        .withColumn("pickup_hour", hour(col("start_time"))) \
        .groupBy("start_geo_hash", "pickup_hour") \
        .agg(
            count("*").alias("total_trips"),
            round(avg("Fare_Amt"), 2).alias("avg_fare"),
            round(avg("Trip_Distance"), 2).alias("avg_recorded_distance"),
            round(avg("haversine_miles"), 2).alias("avg_haversine_distance")
        )

    window_spec = Window.partitionBy("start_geo_hash").orderBy("pickup_hour").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df_gold_final = df_gold_summary.withColumn("running_avg_fare", round(avg("avg_fare").over(window_spec), 2))

    # Append into Gold Layer in HDFS
    df_gold_final.write \
        .mode("append") \
        .option("compression", "snappy") \
        .parquet("hdfs://localhost:9000/data/gold/rides_summary/")

    print("Batch finished processing successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
