import os


from pyspark import SparkContext, SparkConf, SQLContext, HiveContext
from pyspark.sql import SparkSession


master = 'local'
appName = 'creating report tables'


warehouse_location = 'hdfs://localhost:9000/user/hive/warehouse'
metastore_location = '/home/hadoop/apache-hive-2.3.9-bin'

# New way
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive ") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.sql.catalogImplementation", "hive") \
    .config('hive.metastore.warehouse.dir', metastore_location) \
    .enableHiveSupport() \
    .getOrCreate()

if SparkSession.sparkContext:
    print('===============')
    print(f'AppName: {spark.sparkContext.appName}')
    print(f'Master: {spark.sparkContext.master}')
    print('===============')
else:
    print('Could not initialise pyspark session')


spark.sql("CREATE DATABASE IF NOT EXISTS report")
spark.sql("use stage;")


#reports are created to be stored in report db

spark.sql("create table if not exists report.DeptWise_stay as select Department , stay, count(stay) as stay_Frequency "
          "from Department_partitioned group by Department,stay ;")

spark.sql("create table if not exists report.severityWise_stay as select Severity_of_Illness , stay, count(stay) as "
          "stay_Frequency from Severity_partitioned group by Severity_of_Illness,stay ;")

spark.sql("create table if not exists report.DeptWise_rooms as select Department , "
          "sum(Available_Extra_Rooms_in_Hospital) as vacant_rooms from Department_partitioned group by Department;")

spark.sql("create table if not exists report.DeptWise_visitors as select Department , sum(Visitors_with_Patient) as "
          "visitors from Department_partitioned group by Department;")

spark.sql("create table if not exists report.severityWise_age as select Severity_of_Illness , Age_Range, "
          "count(Age_Range) as Age_Frequency from Severity_partitioned group by Severity_of_Illness,Age_Range ;")




