#!/bin/bash

./bin/spark-submit --conf spark.sql.shuffle.partitions=500 --conf spark.default.parallelism=500