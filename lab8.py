import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import  col,  to_timestamp

from lab8util import get_total_price_and_sales, write_output, flatten_json_df, get_json_df, convert_event_time


def create_streaming_df(topic_name: str, bootstrap_servers="127.0.0.1:9092", starting_offsets="earliest",
                        include_headers="true"):
    # Create the streaming_df to read from kafka
    return ((spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", bootstrap_servers)
             .option("subscribe", topic_name))
            .option("startingOffsets", starting_offsets)
            .option("includeHeaders", include_headers)
            .load())


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
            .getOrCreate())


if __name__ == '__main__':
    args = get_args()

    args_dict = vars(args)

    bucket = get_arg(args_dict, "bucket")
    folder = get_arg(args_dict, "folder")
    topic = get_arg(args_dict, "topic")

    print(f"params: bucket: {bucket}, folder: {folder}, topic: {topic}")

    spark = get_spark_session()

    streaming_df = create_streaming_df(topic)
    streaming_df.printSchema()

    json_df = get_json_df(streaming_df)
    json_df.printSchema()

    flattened_df = flatten_json_df(json_df)
    flattened_df.printSchema()

    df_with_event_time = convert_event_time(flattened_df)

    agg_query = get_total_price_and_sales(df_with_event_time)

    print("printing aggregation result")

    output_path = bucket + "/" + folder

    write_output(agg_query, output_path, manual_interuption=True)
