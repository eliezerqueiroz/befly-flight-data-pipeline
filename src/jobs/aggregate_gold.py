import logging
import sys
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


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

def create_airline_kpis(df: DataFrame) -> DataFrame:
    """
    Calcula métricas de pontualidade por companhia aérea.
    """
    logger.info("Aggregating KPIs by Airline...")
    
    return df.groupBy("AIRLINE_NAME").agg(
        F.count("*").alias("total_flights"),
        F.sum("CANCELLED").alias("total_cancelled"),
        # Média de atraso apenas para voos que não foram cancelados
        F.avg("ARRIVAL_DELAY").alias("avg_arrival_delay"),
        # Taxa de cancelamento (Ex: 5.00 = 5%)
        F.round((F.sum("CANCELLED") / F.count("*"))* 100, 2).alias("cancellation_rate"),
        # Contagem de voos com status 'Delayed'
        F.sum(F.when(F.col("FLIGHT_STATUS") == "Delayed", 1).otherwise(0)).alias("total_delayed_flights"),
        # Taxa de atraso (Ex: 10.50 = 10.5%)
        F.round((F.sum("total_delayed_flights") / F.count("*"))* 100, 2).alias("delay_rate")
    ).orderBy(F.col("avg_arrival_delay").desc()) # Ordenar do maior atraso para o menor

def create_daily_summary(df: DataFrame) -> DataFrame:
    """
    Calcula resumo diário de operações.
    """
    logger.info("Aggregating Daily Summary...")
    
    return df.groupBy("FLIGHT_DATE").agg(
        F.count("*").alias("total_flights"),
        F.sum("CANCELLED").alias("total_cancelled"),
        F.avg("DEPARTURE_DELAY").alias("avg_departure_delay")
    ).orderBy("FLIGHT_DATE")

def main():
    logger.info("Starting Gold Layer Aggregation...")
    
    spark = create_spark_session("BeFly_Gold_Aggregation")
    spark.sparkContext.setLogLevel("WARN")

    base_path = os.getcwd()
    silver_path = os.path.join(base_path, "data", "silver", "flights_enriched")
    gold_path = os.path.join(base_path, "data", "gold")

    # 1. Read Silver Data
    try:
        logger.info(f"Reading Silver data from: {silver_path}")
        df_silver = spark.read.parquet(silver_path)
    except Exception as e:
        logger.error(f"Failed to read silver data: {e}")
        sys.exit(1)

    # 2. Create Aggregations
    df_airline_kpis = create_airline_kpis(df_silver)
    df_daily_summary = create_daily_summary(df_silver)

    # 3. Write to Gold
    # Salvar KPI de Companhias
    airline_output = os.path.join(gold_path, "airline_performance")
    logger.info(f"Writing Airline KPIs to: {airline_output}")
    df_airline_kpis.write.mode("overwrite").parquet(airline_output)
    
    # Salvar Resumo Diário
    daily_output = os.path.join(gold_path, "daily_summary")
    logger.info(f"Writing Daily Summary to: {daily_output}")
    df_daily_summary.write.mode("overwrite").parquet(daily_output)
    
    # Bonus: Mostrar uma prévia no log para validação rápida
    logger.info("Preview of Airline KPIs:")
    df_airline_kpis.show(5, truncate=False)

    logger.info("Preview of Daily Summary:")
    df_daily_summary.show(5, truncate=False)

    logger.info("Gold Layer Aggregation finished successfully.")
    spark.stop()

if __name__ == "__main__":
    main()