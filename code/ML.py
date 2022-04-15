
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import RandomForestClassifier



appName = "210940125053_Mini_project_ml"
sparkSession = SparkSession.builder.appName(appName).getOrCreate()

if SparkSession.sparkContext:
    print('===============')
    print(f'AppName: {sparkSession.sparkContext.appName}')
    print(f'Master: {sparkSession.sparkContext.master}')
    print('===============')
else:
    print('Could not initialise pyspark session')

df=sparkSession.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", ",") \
    .parquet('hdfs://localhost:9000/miniproject/persist')


#Indexing categorical variables and removing extra columns including input of string indexer as it will be repeated

indexer=StringIndexer(inputCols=['Hospital_type_code','City_Code_Hospital','Hospital_region_code','Department','Ward_Type','Type_of_Admission','Severity_of_Illness','Age_Range','Stay'],outputCols=['Hospital_type','City','region','Department_index','Ward_Type_indexed','Admission_type','Severity','Age_Range_index','Stay_index'])
cols=['case_id','Hospital_code','Hospital_type_code','City_Code_Hospital','Hospital_region_code','Department','Ward_Type','Ward_Facility_code','patientid','City_Code_Patient','Admission_Deposit','Type_of_Admission','Severity_of_Illness','Age_Range','Stay']
df=indexer.fit(df).transform(df).drop(*cols)

#vector assembler takes all columns except stay columns that is to be predicted
assembler = VectorAssembler(inputCols=(df.select([c for c in df.columns if c not in {'Stay_index'}])).columns, outputCol="features").setHandleInvalid("skip")


#gradient boost tree classifier model to predict the range of stay in hospital

rfc = RandomForestClassifier(featuresCol="features",labelCol="Stay_index", numTrees=20)


pipeline = Pipeline(stages=[assembler,rfc])
model = pipeline.fit(df)


#saving the model to hdfs
model.save('hdfs://localhost:9000/miniproject/Ml_Model')
