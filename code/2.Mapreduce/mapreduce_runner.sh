#!/bin/bash

cd $Hadoop_HOME

hadoop jar /home/vedant/eclipse-workspace/Mapreduce/target/Mapreduce-0.0.1-SNAPSHOT.jar Mini_Project.Mapreduce_job /miniproject/stage/part-00000-76967e95-c875-4b24-abfe-929717ca2516-c000.csv /miniproject/dq_good

