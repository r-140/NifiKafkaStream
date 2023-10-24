import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, DoubleType


def get_json_schema():
    json_schema = StructType([StructField('event', StringType(), True),
                              StructField('data',
                                          StructType([
                                              StructField('bitstamps',
                                                          ArrayType(
                                                              StructType([
                                                                  StructField('id', StringType(), True),
                                                                  StructField('order_type',
                                                                              StringType(), True),
                                                                  StructField('amount', DoubleType(),
                                                                              True),
                                                                  StructField('amount_traded',
                                                                              DoubleType(), True),
                                                                  StructField('amount_at_create',
                                                                              DoubleType(), True),
                                                                  StructField('price', DoubleType(),
                                                                              True)
                                                              ]),
                                                              True),
                                                          True)]),
                                          True),
                              StructField('eventTime', StringType(), True)])
    return json_schema


def create_streaming_df(spark: SparkSession, topic_name: str):
    # Create the streaming_df to read from kafka
    streaming_df = ((spark.readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                     .option("subscribe", "nain_test_topic"))
                    .option("startingOffsets", "earliest")
                    .load())
    return streaming_df
    # .withWatermark("eventTime", "1 minute"))


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", help="gcp bucket where result will be saved")
    parser.add_argument("--folder", help="folder in bucket where result will be saved")
    parser.add_argument("--topic", help="kafka topic from where data will be read")
    return parser.parse_args()


def get_arg(args_dict, arg_name):
    if args_dict[arg_name] is None:
        raise RuntimeError(arg_name + " parameter must be specified")
    return args_dict[arg_name]


def get_spark_session():
    return (SparkSession.builder
            .appName("streaming_tasks")
            .config("spark.streaming.stopGracefullyOnShutdown", True)
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1')
            .config("spark.sql.shuffle.partitions", 3)
            .getOrCreate())


if __name__ == '__main__':
    args = get_args()

    args_dict = vars(args)

    bucket = get_arg(args_dict, "bucket")
    folder = get_arg(args_dict, "folder")
    topic = get_arg(args_dict, "topic")

    spark = get_spark_session()

    streaming_df = create_streaming_df(spark, topic)
    print("showing streaming_df")
    streaming_df.show(5)

    json_df = streaming_df.selectExpr("cast(value as string) as value")
    print("showing json df")
    json_df.show(5)
    from pyspark.sql.functions import from_json

    json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], get_json_schema())).select("value.*")

    print("showing json expanded df")
    json_expanded_df.show(10)

    # JSON Schema
