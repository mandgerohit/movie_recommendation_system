#!/bin/bash
cd $SPARK_HOME
./bin/spark-submit --master spark://152.46.19.250:7077 --executor-memory 2G --total-executor-cores 72 /home/rnmandge/final_machine_learning/Recommender/recommender.py $1 $2 $3 1000
