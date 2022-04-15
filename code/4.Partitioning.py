import os


from pyspark import SparkContext, SparkConf, SQLContext, HiveContext
from pyspark.sql import SparkSession


master = 'local'
appName = 'creating partitioned tables'


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


spark.sql("CREATE DATABASE IF NOT EXISTS stage")
spark.sql("use stage;")

spark.sql("drop table staging")


spark.sql("create external table if not exists staging (case_id Integer , Hospital_code String, Hospital_type_code "
          "String,City_Code_Hospital String, Hospital_region_code String, Available_Extra_Rooms_in_Hospital Integer ,"
          "Department String, Ward_Type String, Ward_Facility_code String,Bed_Grade Double, "
          "patientid Integer,City_Code_Patient String,Type_of_Admission String,Severity_of_Illness String,"
          "Visitors_with_Patient Integer, Age_Range String, "
          "Admission_Deposit String,Stay  String) stored as parquet location '/miniproject/persist';")

# spark.sql("select * from staging limit 10").show()
#partition based on department
spark.sql("create external table if not exists Department_partitioned (case_id Integer , Hospital_code String, Hospital_type_code "
          "String,City_Code_Hospital String, Hospital_region_code String, Available_Extra_Rooms_in_Hospital Integer ,"
          "Ward_Type String, Ward_Facility_code String,Bed_Grade Double,patientid Integer,City_Code_Patient String,"
          "Type_of_Admission String,Severity_of_Illness String,Visitors_with_Patient Integer, Age_Range String, "
          "Admission_Deposit String,Stay  String) PARTITIONED BY (Department String) stored as parquet location '/miniproject/partition/dept';")


spark.sql("insert overwrite table Department_partitioned partition(Department) select case_id ,Hospital_code , "
          "Hospital_type_code ,City_Code_Hospital , Hospital_region_code , Available_Extra_Rooms_in_Hospital, "
          "Ward_Type , Ward_Facility_code ,Bed_Grade , "
          "patientid ,City_Code_Patient ,Type_of_Admission ,Severity_of_Illness ,Visitors_with_Patient , Age_Range , "
          "Admission_Deposit, Stay, Department from staging;")

#partitioning by severity

spark.sql("create external table if not exists Severity_partitioned (case_id Integer , Hospital_code String, Hospital_type_code "
          "String,City_Code_Hospital String, Hospital_region_code String, Available_Extra_Rooms_in_Hospital Integer ,"
          "Department String,Ward_Type String, Ward_Facility_code String,Bed_Grade Double,patientid Integer,City_Code_Patient String,"
          "Type_of_Admission String,Visitors_with_Patient Integer, Age_Range String, "
          "Admission_Deposit String,Stay  String) PARTITIONED BY (Severity_of_Illness String) stored as parquet location '/miniproject/partition/severity';")


spark.sql("insert overwrite table severity_partitioned partition(Severity_of_Illness) select case_id ,Hospital_code , "
          "Hospital_type_code ,City_Code_Hospital , Hospital_region_code , Available_Extra_Rooms_in_Hospital, "
          "Department, Ward_Type , Ward_Facility_code ,Bed_Grade , "
          "patientid ,City_Code_Patient ,Type_of_Admission ,Visitors_with_Patient , Age_Range , "
          "Admission_Deposit, Stay, Severity_of_Illness  from staging;")


