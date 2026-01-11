import logging
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
import sys

sys.path.append(os.getcwd())
from src.utils import create_spark_session

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def read_csv(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Reads a CSV file into a Spark DataFrame using inferSchema.
    """
    logger.info(f"Reading CSV file from: {file_path}")
    return spark.read.csv(file_path, header=True, inferSchema=True)

def write_parquet(df: DataFrame, output_path: str, mode: str = "overwrite") -> None:
    """
    Writes a DataFrame to Parquet format.
    """
    logger.info(f"Writing data to Parquet at: {output_path}")
    df.write.mode(mode).parquet(output_path)
    logger.info("Write operation completed.")

def main():
    logger.info("Starting Bronze Layer Ingestion...")

    # Define paths (relative paths for local execution)
    # In a real scenario, these would be S3 paths or passed via arguments
    base_path = os.getcwd()
    raw_path = os.path.join(base_path, "data", "raw")
    bronze_path = os.path.join(base_path, "data", "bronze")

    spark = create_spark_session("BeFly_Bronze_Ingestion")

    # 1. Process Airlines
    try:
        airlines_df = read_csv(spark, os.path.join(raw_path, "airlines.csv"))
        write_parquet(airlines_df, os.path.join(bronze_path, "airlines"))
    except Exception as e:
        logger.error(f"Error processing airlines: {e}")

    # 2. Process Airports
    try:
        airports_df = read_csv(spark, os.path.join(raw_path, "airports.csv"))
        write_parquet(airports_df, os.path.join(bronze_path, "airports"))
    except Exception as e:
        logger.error(f"Error processing airports: {e}")

    # 3. Process Flights (With Filter optimization)
    try:
        logger.info("Processing Flights data with filter (MONTH=1)...")
        flights_df = read_csv(spark, os.path.join(raw_path, "flights.csv"))
        
        # Applying the filter as requested to optimize local processing
        flights_filtered_df = flights_df.filter("MONTH = 1")
        
        logger.info(f"Filtered flights count: {flights_filtered_df.count()}")
        
        write_parquet(flights_filtered_df, os.path.join(bronze_path, "flights"))
    except Exception as e:
        logger.error(f"Error processing flights: {e}")

    logger.info("Bronze Layer Ingestion finished successfully.")
    spark.stop()

if __name__ == "__main__":
    main()