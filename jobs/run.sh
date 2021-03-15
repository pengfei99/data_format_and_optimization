#!/bin/bash
END=3
FUNC="stats"

for ((i = 1; i <= END; i++)); do
  for fmt in csv json avro parquet; do
    spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.1 script.py $fmt $FUNC
  done
done
