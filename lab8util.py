import pyspark.sql.functions as f


def add_sales_col(df):
    return df.withColumn("sales", f.col("price") * f.col("amount"))


def get_total_price_and_sales(df):
    return df.withColumn("sales", f.col("price") * f.col("amount")) \
        .groupBy("eventTime") \
        .agg({'price': 'sum', 'sales': 'sum'})
