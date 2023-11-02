import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructField, StructType, DoubleType, IntegerType


def get_total_price_and_sales(df):
    return df.withColumn("sales", f.col("price") * f.col("amount")) \
        .withWatermark("event_time", "5 years") \
        .groupBy(f.window("event_time", "1 minute")) \
        .agg({'*': 'count', 'price': 'avg', 'sales': 'sum'}) \
        .withColumnRenamed("sum(sales)", "total_sales") \
        .withColumnRenamed("count(1)", "total_records") \
        .withColumnRenamed("avg(price)", "avg_price")


def write_output(df, output_path, format='parquet', output_mode="append", manual_interuption=False):
    writing_df = df.writeStream \
        .format(format) \
        .option("parquet.block.size", 1024) \
        .option("path", output_path) \
        .outputMode(output_mode) \
        .option("checkpointLocation", output_path) \
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
