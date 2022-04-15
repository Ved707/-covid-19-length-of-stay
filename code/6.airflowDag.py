from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id='210940125053_MiniProjectDag',
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['dbda','miniproject'],
    params={"Course": "BDT"},
) as dag:
    sparkPersist = SparkSubmitOperator(
    task_id='Spark-task',
    name="airflow-spark",
    application='/home/vedant/DBDA_HOME/DBDA_Code/spark/pythonProject/miniproject/Spark_to_persist.py',
    application_args=['1'],
    env_vars={'PYSPARK_DRIVER_PYTHON': 'python',
              'HADOOP_CONF_DIR': '/home/vedant/DBDA_HOME/hadoop-3.3.1/etc/hadoop',
              'PYSPARK_PYTHON': '/usr/bin/python3',
              'AIRFLOW_CONN_SPARK_DEFAULT': 'local'},
    )

    Mapreduce = BashOperator(
    task_id='Map_reduce',
    bash_command='/home/vedant/miniproject/code/2.Mapreduce/mapreduce_runner.sh',
    )


    hive_partitoning = BashOperator(task_id="hive_partitioningg",
                               bash_command="spark-submit --master local Partitioning.py",
                               cwd="/home/vedant/DBDA_HOME/DBDA_Code/spark/pythonProject/miniproject",
                               dag=dag)

    hive_reporting = BashOperator(task_id="reporting ",
                                     bash_command="spark-submit --master yarn reporting.py",
                                     cwd="/home/vedant/DBDA_HOME/DBDA_Code/spark/pythonProject/miniproject",
                                     dag=dag)

    pysaprk_ML = BashOperator(task_id="machine_learaning",
                              bash_command="spark-submit --master local ml.py ",
                              cwd="/home/harsh/DBDA_HOME/DBDA_CODE/SPARK/pythonProject1/miniproject",
                              dag=dag)



Mapreduce >> sparkPersist
sparkPersist >> hive_partitoning
sparkPersist >> pysaprk_ML
hive_partitoning >> hive_reporting

#Sparksql operator  giving skewied results and HiveOperator showing error while impoerting