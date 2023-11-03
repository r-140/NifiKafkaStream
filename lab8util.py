import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructField, StructType, DoubleType, IntegerType


def get_total_price_and_sales(df, window_duration = "1 minute", watermark_delay_threshold = "3 minutes"):
    return df.withColumn("sales", f.col("price") * f.col("amount")) \
        .withWatermark("event_time", watermark_delay_threshold) \
        .groupBy(f.window("event_time", window_duration)) \
        .agg({'*': 'count', 'price': 'avg', 'sales': 'sum'}) \
        .withColumnRenamed("sum(sales)", "total_sales") \
        .withColumnRenamed("count(1)", "total_records") \
        .withColumnRenamed("avg(price)", "avg_price")


def write_output(df, output_path, query_name,  format='parquet', output_mode="append", manual_interuption=False):
    writing_df = df.writeStream \
        .format(format) \
        .queryName(query_name) \
        .option("parquet.block.size", 1024) \
        .option("path", output_path + "/output") \
        .outputMode(output_mode) \
        .option("checkpointLocation", output_path + "/checkpoint_dir") \
        .start()
    if manual_interuption:
        writing_df.awaitTermination()


def get_json_schema():
    json_schema = StructType([
        StructField('event', StringType(), True),
        StructField('data', StructType([
            StructField('id', StringType(), True),
            StructField('order_type', IntegerType(), True),
            StructField('datetime', StringType(), True),
            StructField('amount', DoubleType(), True),
            StructField('amount_traded', StringType(), True),
            StructField('amount_at_create', StringType(), True),
            StructField('price', DoubleType(), True)
        ])
                    )
    ])
    return json_schema


def flatten_json_df(json_df):
    return (json_df
            .selectExpr("data.id as id",
                        "data.datetime as datetime",
                        "data.amount as amount",
                        "data.price as price")
            ).dropDuplicates(["id"])


def get_json_df(streaming_df):
    json_df = streaming_df.selectExpr("cast(value as string) as value")
    return json_df.withColumn("value", f.from_json(json_df["value"], get_json_schema())).select("value.*")


def convert_event_time(df):
    return (df.withColumn("event_time",
                          f.to_utc_timestamp(f.from_unixtime(f.col("datetime")), 'EST'))
            .drop("datetime"))