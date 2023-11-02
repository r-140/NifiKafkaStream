import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructField, StructType, ArrayType, DoubleType, LongType, IntegerType


def get_total_price_and_sales(df):
    return df.withColumn("sales", f.col("price") * f.col("amount")) \
        .groupBy(f.window("event_time", "1 minute")) \
        .agg({'*': 'count', 'price': 'sum', 'sales': 'sum'})


# def write_output(df, output_path, format='parquet', output_mode="complete", manual_interuption=False):
#     # Write the output to console sink to check the output
#     writing_df = df.writeStream \
#         .format(format) \
#         .outputMode(output_mode) \
#         .option("checkpointLocation", "checkpoint_dir") \
#         .start()
#     if manual_interuption:
#         writing_df.awaitTermination()


def write_output(df, output_path, format='parquet', output_mode = "complete", manual_interuption = False):
    # Write the output to console sink to check the output
    writing_df = df.writeStream \
        .format(format) \
        .option("path", output_path) \
        .outputMode(output_mode) \
        .start()
    # todo clarify whether the trigger needed
    # .trigger("1 minute") \
    # print("writing to output file")
    # writing_df.
    # Start the streaming application to run until the following happens
    # 1. Exception in the running program
    # 2. Manual Interruption
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
