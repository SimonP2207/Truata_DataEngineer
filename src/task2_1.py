from pyspark.sql import SparkSession
import config as cfg

# Read in parquet file to dataframe
spark = SparkSession.builder \
                    .appName("task2") \
                    .getOrCreate()
dataframe = spark.read.parquet(str(cfg.files['airbnb']))
