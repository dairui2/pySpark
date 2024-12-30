from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkOnHive").master("yarn").enableHiveSupport().getOrCreate()
spark.sql("select * from db1.hive_01").show()