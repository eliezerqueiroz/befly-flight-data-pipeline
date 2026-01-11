import logging
from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    """
    Cria e retorna uma SparkSession configurada para o ambiente.
    Centraliza as configurações de memória e particionamento.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .master("local[*]") \
        .getOrCreate()