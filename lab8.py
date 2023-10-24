import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, DoubleType
from pyspark.sql.functions import from_json, explode, col, to_date, sum


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

def write_output(df):
    # Write the output to console sink to check the output
    writing_df = df.writeStream \
        .format("console") \
        .option("checkpointLocation", "checkpoint_dir") \
        .outputMode("complete") \
        .start()

    # Start the streaming application to run until the following happens
    # 1. Exception in the running program
    # 2. Manual Interruption
    writing_df.awaitTermination()

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
        .select("event", "eventTime", "data") \
        .withColumn("bitstamps", explode("data.bitstamps")) \
        .drop("data")

    print("printing exploded df")
    print(exploded_df)

    # Flatten the exploded df
    flattened_df = exploded_df \
        .selectExpr("event", "cast(eventTime as timestamp) as eventTime",
                    "bitstamps.amount as amount",
                    "bitstamps.amount_traded as amount_traded", "bitstamps.price as price")


    print("printing flatteden df")
    print(flattened_df)

    sum_df = flattened_df.where("event = 'order_created'") \
        .withColumn("sales", col("price")*col("amount")) \
        .groupBy("eventTime") \
        .agg(sum("price").alias("sum_price"), sum("sales").alias("total_sales"))

    write_output(sum_df)


