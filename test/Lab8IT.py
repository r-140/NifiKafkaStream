import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from lab8util import get_json_df, flatten_json_df, convert_event_time, get_total_price_and_sales

spark = SparkSession.builder.appName("streaming_app").master('local[*]').getOrCreate()

schema = StructType([
    StructField('timestamp', StringType(), True),
    StructField('key', StringType(), True),
    StructField('value', StringType(), True)
])


class TestStreaming(unittest.TestCase):
    def test_aggregate(self):
        streaming_df = spark.readStream.schema(schema).text('test_data.txt')
        print(streaming_df.isStreaming)

        json_df = get_json_df(streaming_df)

        flattened_df = flatten_json_df(json_df)

        df_with_event_time = convert_event_time(flattened_df)

        agg_query = get_total_price_and_sales(df_with_event_time)

        initDF = (agg_query.writeStream
                  .outputMode("append")
                  .format("memory")
                  .queryName("initDF")
                  .start())

        res_df = spark.sql("select * from initDF").show()


        initDF.awaitTermination()

        self.assertEqual(res_df["avg_price"], 10)
        self.assertEqual(res_df["total_sales"], 10)
        self.assertEqual(res_df["total_records"], 10)




if __name__ == '__main__':
    unittest.main()
