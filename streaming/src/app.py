#!/usr/bin/env python

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import re
import sys

from .config import AppConfig
from.parser import parse_line

def _setup():
    sc = SparkContext(appName=AppConfig.appName)
    ssc = StreamingContext(sc, AppConfig.batchDuration)
    ssc.checkpoint("nkdhnyCheckpointKafka")

    return KafkaUtils.createStream(
            ssc, AppConfig.zk,
            AppConfig.appId,
            {AppConfig.topic: AppConfig.partitions}
    ), ssc, sc

def main():

    log_dstream, ssc, sc = _setup()

    errors = log_dstream.map(parse_line).cache().filter(lambda rec: rec['code'] != 200)

    errors.window(1).pprint()

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()

