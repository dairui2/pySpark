from pyspark.sql import SparkSession

#➜  ~ vi ./.bash_profile
# export HADOOP_USER_NAME=hive
spark = SparkSession.builder.appName("SparkOnMySQL").enableHiveSupport().getOrCreate()
spark.sql("create table db1.spark_on_mysql(id int, word varchar(20))")
spark.sql("insert into db1.spark_on_mysql values(3, 'Spark')")
spark.sql("select * from db1.spark_on_mysql").show()