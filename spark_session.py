from pyspark.sql import SparkSession

def get_spark_session(app_name: str, master: str = "local[*]"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    return spark
  
