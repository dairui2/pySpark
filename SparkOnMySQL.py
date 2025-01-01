from pyspark.sql import SparkSession
import os
os.environ['SPARK_EXECUTOR_MEMORY'] = '2g'  # 设置每个executor的内存为4GB
os.environ['SPARK_DRIVER_MEMORY'] = '1g'  # 设置driver的内存为2GB

#➜  ~ vi ./.bash_profile
# export HADOOP_USER_NAME=hive
spark = SparkSession.builder.appName("SparkOnMySQL").master("yarn").enableHiveSupport().getOrCreate()
# spark.sql("create table db1.spark_on_mysql(id int, word varchar(20))")
spark.sql("insert into db1.spark_on_mysql values(4, 'spark')")
spark.sql("select * from db1.spark_on_mysql").show()