from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import col, when, lit, desc
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StructType, StringType, DoubleType

master = 'local'
appName = 'staging to persist layer'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)
# You will need to create the sqlContext
sqlContext = SQLContext(sc)
ss = SparkSession(sc)
# sparkSession = SparkSession.builder.appName(appName).master(master).getOrCreate()
# sc = sparkSession.sparkContext

if ss:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

HealthSchema = StructType([StructField('case_id',IntegerType(), True),
                            StructField('Hospital_code', StringType(), True),
                            StructField('Hospital_type_code', StringType(), True),
                            StructField('City_Code_Hospital', StringType(), True),
                            StructField('Hospital_region_code', StringType(), True),
                            StructField('Available_Extra_Rooms_in_Hospital', IntegerType(), True),
                            StructField('Department', StringType(), True),
                            StructField('Ward_Type', StringType(), True),
                            StructField('Ward_Facility_code', StringType(), True),
                            StructField('Bed_Grade', DoubleType(), True),
                            StructField('patientid', IntegerType(), True),
                            StructField('City_Code_Patient', StringType(), True),
                            StructField('Type_of_Admission', StringType(), True),
                            StructField('Severity_of_Illness', StringType(), True),
                            StructField('Visitors_with_Patient', IntegerType(), True),
                            StructField('Age_Range', StringType(), True),
                            StructField('Admission_Deposit', StringType(), True),
                            StructField('Stay', StringType(), True)])



df=ss.read.schema(HealthSchema).format("csv").option("header", "False").option("inferSchema", "true").load('hdfs://localhost:9000/miniproject/dq_good')
print(df.columns)
df.printSchema()
df.write.option("header", "true").mode('overwrite').parquet('hdfs://localhost:9000/miniproject/persist')
