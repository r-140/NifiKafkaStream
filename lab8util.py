import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructField, StructType, DoubleType, IntegerType


def get_total_price_and_sales(df):
    return df.withColumn("sales", f.col("price") * f.col("amount")) \
        .withWatermark("time", "5 years") \
        .groupBy(f.window("event_time", "1 minute")) \
        .agg({'*': 'count', 'price': 'sum', 'sales': 'sum'})


def write_output(df, output_path, format='parquet', output_mode = "append", manual_interuption = False):
    # Write the output to console sink to check the output
    writing_df = df.writeStream \
        .format(format) \
        .option("parquet.block.size", 1024) \
        .option("path", output_path) \
        .outputMode(output_mode) \
        .option("checkpointLocation", "checkpoint_dir") \
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
            StructField('amount', DoubleType(),True),
            StructField('amount_traded', StringType(), True),
            StructField('amount_at_create', StringType(), True),
            StructField('price', DoubleType(), True)
        ])
                    )
    ])
    return json_schema
