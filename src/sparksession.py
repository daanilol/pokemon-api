from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]") \
                    .appName('my_spark_session') \
                    .getOrCreate()