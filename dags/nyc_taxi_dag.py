from datetime import datetime, timedelta
import pandas as pd
import duckdb

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "shibin",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# jan 2024 - picked this month randomly, seemed to have good data
PARQUET_URL  = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
TLC_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DB_PATH      = "/opt/airflow/data/nyc_taxi_warehouse.db"
RAW_PATH     = "/tmp/nyc_raw.parquet"
CLEAN_PATH   = "/tmp/nyc_clean.parquet"


def extract_taxi_data(**ctx):
    df = pd.read_parquet(PARQUET_URL)
    print(f"Extracted {len(df)} raw trips")
    df.to_parquet(RAW_PATH, index=False)


def transform_clean(**ctx):
    df = pd.read_parquet(RAW_PATH)
    df = df[[
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "fare_amount",
        "tip_amount", "total_amount", "payment_type",
        "PULocationID", "DOLocationID",
    ]].copy()
    df = df[
        # also drop trips > 2hrs, probably meter left running
        (df["trip_distance"] > 0) & (df["fare_amount"] > 0) &
        (df["total_amount"] > 0) & (df["passenger_count"] > 0)
    ].dropna()
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day"]  = df["tpep_pickup_datetime"].dt.day_name()
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
    df["trip_duration_mins"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    ).round(2)
    df = df[(df["trip_duration_mins"] > 0) & (df["trip_duration_mins"] < 120)]
    payment_map = {1: "Credit Card", 2: "Cash", 3: "No Charge", 4: "Dispute"}
    df["payment_type_name"] = df["payment_type"].map(payment_map).fillna("Unknown")
    print(f"Clean rows: {len(df)}")
    df.to_parquet(CLEAN_PATH, index=False)


def build_star_schema(**ctx):
    import hashlib
    df = pd.read_parquet(CLEAN_PATH)

    # dim_location — real TLC zone lookup (fixes hardcoded borough/zone)
    zone_df = pd.read_csv(TLC_ZONE_URL).rename(columns={
        "LocationID": "location_id",
        "Borough": "borough",
        "Zone": "zone",
        "service_zone": "service_zone",
    })

    # fact_trips — hash surrogate key (fixes range())
    fact_trips = df[[
        "pickup_date", "pickup_hour", "PULocationID", "DOLocationID",
        "passenger_count", "trip_distance", "fare_amount",
        "tip_amount", "total_amount", "trip_duration_mins", "payment_type",
    ]].copy()
    # was using itertuples() which is slow on 3M rows, switched to vectorized
    # switched to vectorized after noticing the dag was timing out
    # vectorized surrogate key — much faster than itertuples() on millions of rows
    fact_trips["trip_id"] = (
        fact_trips["pickup_date"].astype(str)
        + fact_trips["pickup_hour"].astype(str)
        + fact_trips["PULocationID"].astype(str)
        + fact_trips["total_amount"].astype(str)
    ).apply(lambda s: hashlib.md5(s.encode()).hexdigest()[:16])

    dim_time = df[["pickup_date", "pickup_hour", "pickup_day"]].drop_duplicates().copy()
    dim_time["time_id"] = range(1, len(dim_time) + 1)
    dim_time["is_weekend"] = dim_time["pickup_day"].isin(["Saturday", "Sunday"])
    dim_time["time_of_day"] = dim_time["pickup_hour"].apply(
        lambda x: "Morning" if 6 <= x < 12 else "Afternoon" if 12 <= x < 17
        else "Evening" if 17 <= x < 21 else "Night"
    )


    dim_payment = df[["payment_type", "payment_type_name"]].drop_duplicates().copy()
    dim_payment["payment_id"] = range(1, len(dim_payment) + 1)

    conn = duckdb.connect(DB_PATH)
    for table, frame in [
        ("fact_trips", fact_trips),
        ("dim_time", dim_time),
        ("dim_payment", dim_payment),
        ("dim_location", zone_df),
    ]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM frame")
        rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}: {rows} rows")
    conn.close()


def spark_analysis(**ctx):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("NYC_Taxi").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    df = pd.read_parquet(CLEAN_PATH)
    spark_df = spark.createDataFrame(df)
    spark_df.createOrReplaceTempView("taxi_trips")
    # local mode, shuffle.partitions=4 otherwise spark makes 200 partitions which is overkill
    spark.sql("SELECT pickup_hour, COUNT(*) as trips, ROUND(AVG(fare_amount),2) as avg_fare FROM taxi_trips GROUP BY pickup_hour ORDER BY trips DESC LIMIT 5").show()
    spark.sql("SELECT payment_type_name, COUNT(*) as trips, ROUND(SUM(total_amount),2) as revenue FROM taxi_trips GROUP BY payment_type_name ORDER BY revenue DESC").show()
    spark.stop()


def data_quality_checks(**ctx):
    conn = duckdb.connect(DB_PATH)
    checks = {
        "Negative fares":      "SELECT COUNT(*) FROM fact_trips WHERE fare_amount <= 0",
        "Zero distances":      "SELECT COUNT(*) FROM fact_trips WHERE trip_distance <= 0",
        "Null trip_id":        "SELECT COUNT(*) FROM fact_trips WHERE trip_id IS NULL",
        "Hollow dim_location": "SELECT COUNT(*) FROM dim_location WHERE zone = 'NYC Zone'",
    }
    failed = []
    for label, sql in checks.items():
        count = conn.execute(sql).fetchone()[0]
        print(f"[{'PASS' if count == 0 else 'FAIL'}] {label}: {count}")
        if count > 0:
            failed.append(label)
    conn.close()
    if failed:
        raise ValueError(f"Quality checks failed: {failed}")


with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 3, 17),
    catchup=False,
    tags=["nyc", "taxi", "etl", "duckdb"],  # added pyspark tag after adding spark_analysis task
) as dag:

    t1 = PythonOperator(task_id="extract_taxi_data",   python_callable=extract_taxi_data)
    t2 = PythonOperator(task_id="transform_clean",     python_callable=transform_clean)
    t3 = PythonOperator(task_id="build_star_schema",   python_callable=build_star_schema)
    t4 = PythonOperator(task_id="spark_analysis",      python_callable=spark_analysis)
    t5 = PythonOperator(task_id="data_quality_checks", python_callable=data_quality_checks)

    t1 >> t2 >> t3 >> t4 >> t5












