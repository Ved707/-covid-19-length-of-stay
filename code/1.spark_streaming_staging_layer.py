from pyspark import SparkContext, SparkConf, SQLContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

master = 'local'
appName = 'PySpark_Streaming to load data in staging dir'

config = SparkConf().setAppName(appName).setMaster(master)

ss = SparkSession.builder.appName('MySparkStreamingSession')\
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")\
    .master('local')\
    .getOrCreate()

print(ss.sparkContext.getConf().get("spark.serializer"))
print('Done')

if ss:
    print(ss.sparkContext.appName)
else:
    print('Could not initialise pyspark session')

HealthSchema = StructType([StructField('case_id',IntegerType(), True),
                            StructField('Hospital_code', StringType(), True),
                            StructField('Hospital_type_code', StringType(), True),
                            StructField('City_Code_Hospital', StringType(), True),
                            StructField('Hospital_region_code', StringType(), True),
                            StructField('Available Extra Rooms in Hospital', IntegerType(), True),
                            StructField('Department', StringType(), True),
                            StructField('Ward_Type', StringType(), True),
                            StructField('Ward_Facility_code', StringType(), True),
                            StructField('Bed Grade', DoubleType(), True),
                            StructField('patientid', IntegerType(), True),
                            StructField('City_Code_Patient', StringType(), True),
                            StructField('Type of Admission', StringType(), True),
                            StructField('Severity of Illness', StringType(), True),
                            StructField('Visitors with Patient', IntegerType(), True),
                            StructField('Age_Range', StringType(), True),
                            StructField('Admission_Deposit', StringType(), True),
                            StructField('Stay', StringType(), True)])



HealthData = ss.readStream.schema(HealthSchema)\
    .option('header', True) \
    .csv('/home/vedant/Downloads/archive/healthcare')


query = HealthData.writeStream \
    .outputMode('append')\
    .option('header', True)\
    .option("checkpointLocation",'hdfs://localhost:9000/miniproject/ch1')\
    .format('csv')\
    .option('path','hdfs://localhost:9000/miniproject/stage').start()

query.awaitTermination()

print('=================================')