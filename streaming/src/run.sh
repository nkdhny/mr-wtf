#!/usr/bin/env bash

spark-submit \
      --jars /opt/cloudera/parcels/CDH/jars/spark-streaming-kafka_2.10-1.6.0-cdh5.9.0.jar \
      --master yarn-client \
      --num-executors 2 \
      --executor-cores 1 \
      --executor-memory 2048m \
      --conf 'appName=NkdhnyErrorsCount' app.py