CREATE DATABASE IF NOT EXISTS rides_project;
USE rides_project;

-- Silver external table
CREATE EXTERNAL TABLE IF NOT EXISTS silver_rides (
    trip_id                    STRING,

    Trip_Pickup_DateTime       STRING,
    Trip_Dropoff_DateTime      STRING,

    Passenger_Count            BIGINT,
    Trip_Distance              DOUBLE,

    Start_Lon                  DOUBLE,
    Start_Lat                  DOUBLE,
    End_Lon                    DOUBLE,
    End_Lat                    DOUBLE,

    Fare_Amt                   DOUBLE,

    start_time                 TIMESTAMP,
    end_time                   TIMESTAMP,
    trip_duration_sec          BIGINT,

    day_of_week                INT,
    is_weekend                 INT,

    start_geo_hash             STRING,
    end_geo_hash               STRING,

    haversine_miles            DOUBLE
)
PARTITIONED BY (
    pickup_year                INT,
    pickup_month               INT
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/data/staging/rides_geo/';

-- Gold external table
CREATE EXTERNAL TABLE IF NOT EXISTS gold_rides_summary (
    start_geo_hash             STRING,
    pickup_hour                INT,

    total_trips                BIGINT,
    total_fare                 DOUBLE,
    total_recorded_distance    DOUBLE,
    total_haversine_distance   DOUBLE,

    avg_fare                   DOUBLE,
    avg_recorded_distance      DOUBLE,
    avg_haversine_distance     DOUBLE,

    running_avg_fare           DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/data/gold/rides_summary/';
