import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, DoubleType, LongType
from pyspark.sql.functions import from_json, explode, col, to_date, sum, to_timestamp

from lab8util import get_total_price_and_sales, write_output


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
                                                                  StructField('datetime',
                                                                              LongType(), True),
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


# topic name nain_test_topic
def create_streaming_df(spark: SparkSession, topic_name: str):
    # Create the streaming_df to read from kafka
    streaming_df = ((spark.readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                     .option("subscribe", topic_name))
                    .option("startingOffsets", "earliest")
                    .load())
    return streaming_df
            # .withWatermark("eventTime", "1 minute")



# todo consider whether watermark is required
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

    json_df = streaming_df.selectExpr("cast(value as string) as value")
    print("showing json df")
    print(json_df)

    json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], get_json_schema())).select("value.*")

    print("showing json expanded df")
    print(json_expanded_df)

    exploded_df = json_expanded_df \
        .select("event", "data") \
        .withColumn("bitstamps", explode("data.bitstamps")) \
        .drop("data")


    print("printing exploded df")
    print(exploded_df)

    # Flatten the exploded df
    flattened_df = (exploded_df
                    .selectExpr("bitstamps.id as id",
                                "bitstamps.datetime as datetime",
                                "bitstamps.amount as amount",
                                "bitstamps.price as price")
                    ).dropDuplicates(["id"])

    print("printing flatteden df")
    print(flattened_df)

    df_with_event_time = (flattened_df
                          .withColumn("event_time", to_timestamp(col("datetime")))
                          .drop("datetime"))

    print("printing flatteden df with timestamp")
    print(df_with_event_time)

    sum_df = get_total_price_and_sales(flattened_df)

    output_path = bucket + "/" + folder

    write_output(sum_df, output_path, manual_interuption=True)
