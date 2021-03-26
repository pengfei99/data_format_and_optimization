# spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.1 script.py csv random_batch
# spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.1 script.py parquet write
# du prints kb
# orc 191332 kb
# avro 286628 kb
# parquet 202772 kb
# csv 483432 kb
# json 1292192 kb

# write speed
# orc: 16.25 sec
# avro, write, 13.56
# parquet, write, 13.64
# json, write, 58.96
# csv, write, 13.25
# https://www.kaggle.com/netflix-inc/netflix-prize-data

import time
import argparse
import pyspark
from pyspark.sql import SparkSession
import os
import s3fs


def groupby(sdf):
    return sdf.groupBy("rating").count()


def stats(sdf, field="rating"):
    return sdf.agg({field: "max"}), sdf.agg({field: "min"}), sdf.agg({field: "count"})


def random_batch(sdf):
    return sdf.sample(False, 0.05).collect()


def distinct(sdf):
    return sdf.distinct().count()


def filtering(sdf, date="2005-11-15"):
    return sdf.filter(sdf.date > date).count()


def get_op(op):
    return {
        "stats": stats,
        "random_batch": random_batch,
        "distinct": distinct,
        "filtering": filtering,
        "groupby": groupby,
    }.get(op)


def read(fmt, spark):
    json_data_path = "s3a://pengfei/sspcloud-demo/data_format/netflix.json"
    parquet_data_path = "s3a://pengfei/sspcloud-demo/data_format/netflix.parquet"
    avro_data_path = "s3a://pengfei/sspcloud-demo/data_format/netflix.avro"
    orc_data_path = "s3a://pengfei/sspcloud-demo/data_format/netflix.orc"
    csv_data_path = "s3a://pengfei/sspcloud-demo/data_format/netflix.csv"
    if fmt == "json":
        sdf = spark.read.option("header", "true").json(json_data_path)
    elif fmt == "csv":
        sdf = spark.read.option("header", "true").csv(csv_data_path)
    elif fmt == "avro":
        sdf = spark.read.format("avro").option("header", "true").load(avro_data_path)
    elif fmt == "parquet":
        sdf = spark.read.option("header", "true").parquet(parquet_data_path)
    return sdf


def write(sdf, fmt, name="generated-nf"):
    sdf = sdf.withColumnRenamed("_c0", "user_id") \
        .withColumnRenamed("_c1", "rating") \
        .withColumnRenamed("_c2", "date")
    if fmt == "json":
        sdf.write.option("header", "true").json("{}.json".format(name))
    elif fmt == "csv":
        sdf.write.option("header", "true").csv("{}.csv".format(name))
    elif fmt == "avro":
        sdf.write.format("avro").option("header", "true").save("{}.avro".format(name))
    elif fmt == "parquet":
        sdf.write.option("header", "true").parquet("{}.parquet".format(name))
    elif fmt == "orc":
        sdf.write.option("header", "true").orc("{}.orc".format(name))
    else:
        print("Can't find matched format. Stop spark")


def mute_spark_logs(sc):
    """Mute Spark info logging"""
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("fmt", type=str)
    parser.add_argument("op", type=str)
    args = parser.parse_args()
    endpoint = "https://" + os.environ['AWS_S3_ENDPOINT']
    fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': endpoint})
    fs.info('pengfei/pengfei_test')

    spark = SparkSession.builder.master("k8s://https://kubernetes.default.svc:443").appName("Evaluate data format") \
        .config("spark.kubernetes.container.image", "inseefrlab/jupyter-datascience:master") \
        .config("spark.kubernetes.authenticate.driver.serviceAccountName", os.environ['KUBERNETES_SERVICE_ACCOUNT']) \
        .config("spark.executor.instances", "5") \
        .config("spark.kubernetes.namespace", os.environ['KUBERNETES_NAMESPACE']) \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.0.1") \
        .getOrCreate()

    mute_spark_logs(spark.sparkContext)
    sdf = read("csv", spark)
    sdf.show(5)
    start = time.time()
    write(sdf, args.fmt)
    # get_op(args.op)(sdf)
    print("{}, {}, {}".format(args.fmt, args.op, time.time() - start))
