import logging
import sys
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str) -> SparkSession:
    """
    Creates SparkSession with M1 optimizations.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .master("local[*]") \
        .getOrCreate()

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    logger.info(f"Reading Parquet from: {path}")
    return spark.read.parquet(path)

def clean_flights_data(df: DataFrame) -> DataFrame:
    """
    Applies type casting, handles nulls, and creates derived columns.
    """
    logger.info("Starting data cleaning and standardization...")

    # 1. FLIGHT_DATE Creation (Concat Year-Month-Day -> Date)
    # We use lpad to ensure '1' becomes '01' for months/days
    df_cleaned = df.withColumn(
        "FLIGHT_DATE",
        F.to_date(
            F.concat_ws(
                "-",
                F.col("YEAR"),
                F.col("MONTH"),
                F.col("DAY")
            )
        )
    )

    # 2. Type Casting (Ensure numbers are correct types)
    # Columns to convert to Integer
    int_cols = ["DEPARTURE_DELAY", "ARRIVAL_DELAY", "CANCELLED", "DIVERTED", "DISTANCE"]
    for col_name in int_cols:
        df_cleaned = df_cleaned.withColumn(col_name, F.col(col_name).cast(IntegerType()))

    # 3. Handle Nulls for Delays (Business Rule)
    # If flight is cancelled/diverted, delay is technically None, but for calc we might want 0 or keep null.
    # Strategy: Keep Nulls but ensure flags are 0/1 (handled by cast above)

    # 4. Derived Columns: FLIGHT_STATUS and IS_LONG_DELAY
    df_transformed = df_cleaned.withColumn(
        "FLIGHT_STATUS",
        F.when(F.col("CANCELLED") == 1, "Cancelled")
        .when(F.col("DIVERTED") == 1, "Diverted")
        .when(F.col("ARRIVAL_DELAY") > 15, "Delayed")
        .otherwise("OnTime")
    ).withColumn(
        "IS_LONG_DELAY",
        F.when(F.col("ARRIVAL_DELAY") > 60, True).otherwise(False)
    )

    return df_transformed

def enrich_flights_data(flights: DataFrame, airlines: DataFrame, airports: DataFrame) -> DataFrame:
    """
    Joins flights with airline names and airport details (Origin and Destination).
    """
    logger.info("Starting data enrichment (Joins)...")

    # 1. Join Airlines with Flights
    # Rename the airline name column to avoid confusion
    airlines_clean = airlines.select(
        F.col("IATA_CODE").alias("airline_code"),
        F.col("AIRLINE").alias("AIRLINE_NAME")
    )
    
    df_joined = flights.join(
        airlines_clean,
        flights.AIRLINE == airlines_clean.airline_code,
        "left"
    ).drop("airline_code")

    # 2. Join with Airports (Twice: Origin and Destination)
    
    # Prepare Airport DataFrame (select only needed cols)
    airports_clean = airports.select(
        F.col("IATA_CODE"),
        F.col("AIRPORT"),
        F.col("CITY"),
        F.col("STATE")
    )

    # Alias for Origin
    origin_airports = airports_clean.alias("origin_ap")
    
    # Alias for Destination
    dest_airports = airports_clean.alias("dest_ap")

    # Join Origin and create origin columns
    df_enriched = df_joined.join(
        origin_airports,
        df_joined.ORIGIN_AIRPORT == F.col("origin_ap.IATA_CODE"),
        "left"
    ).select(
        df_joined["*"],
        F.col("origin_ap.AIRPORT").alias("ORIGIN_AIRPORT_NAME"),
        F.col("origin_ap.CITY").alias("ORIGIN_CITY"),
        F.col("origin_ap.STATE").alias("ORIGIN_STATE")
    )

    # Join Destination and create destination columns
    df_final = df_enriched.join(
        dest_airports,
        df_enriched.DESTINATION_AIRPORT == F.col("dest_ap.IATA_CODE"),
        "left"
    ).select(
        df_enriched["*"],
        F.col("dest_ap.AIRPORT").alias("DEST_AIRPORT_NAME"),
        F.col("dest_ap.CITY").alias("DEST_CITY"),
        F.col("dest_ap.STATE").alias("DEST_STATE")
    )

    return df_final

def main():
    logger.info("Starting Silver Layer Transformation...")
    
    spark = create_spark_session("BeFly_Silver_Transformation")
    spark.sparkContext.setLogLevel("WARN")

    base_path = os.getcwd()
    bronze_path = os.path.join(base_path, "data", "bronze")
    silver_path = os.path.join(base_path, "data", "silver", "flights_enriched")

    # 1. Read Bronze Data
    try:
        flights_df = read_parquet(spark, os.path.join(bronze_path, "flights"))
        airlines_df = read_parquet(spark, os.path.join(bronze_path, "airlines"))
        airports_df = read_parquet(spark, os.path.join(bronze_path, "airports"))
    except Exception as e:
        logger.error(f"Failed to read bronze data: {e}")
        sys.exit(1)

    # 2. Clean and Standardize Flights
    flights_clean = clean_flights_data(flights_df)

    # 3. Enrich with Reference Data (Joins)
    flights_enriched = enrich_flights_data(flights_clean, airlines_df, airports_df)

    # 4. Write to Silver
    logger.info(f"Writing enriched data to: {silver_path}")
    flights_enriched.write.mode("overwrite").parquet(silver_path)
    
    logger.info("Silver Layer Transformation finished successfully.")
    spark.stop()

if __name__ == "__main__":
    main()