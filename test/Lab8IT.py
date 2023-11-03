import time
import unittest

from pyspark.sql import SparkSession

from pyspark.sql.types import StringType, StructField, StructType

from lab8util import get_json_df, flatten_json_df, convert_event_time, get_total_price_and_sales

spark = SparkSession.builder.appName("streaming_app").master('local[*]').getOrCreate()

schema = StructType([
    StructField('timestamp', StringType(), True),
    StructField('key', StringType(), True),
    StructField('value', StringType(), True)
])


class TestStreaming(unittest.TestCase):
    def test_aggregate(self):
        streaming_df = spark.readStream.text('data')

        # streaming_df.writeStream.format("console").outputMode("append").start().awaitTermination()

        json_df = get_json_df(streaming_df)

        # json_df.writeStream.format("console").outputMode("append").start().awaitTermination()

        flattened_df = flatten_json_df(json_df)

        # flattened_df.writeStream.format("console").outputMode("append").start().awaitTermination()

        df_with_event_time = convert_event_time(flattened_df)

        # df_with_event_time.writeStream.format("console").outputMode("append").start()

        agg_query = get_total_price_and_sales(df_with_event_time, window_duration="1 second")

        # agg_query.writeStream.format("console").outputMode("complete").start()

        initDF = (agg_query.writeStream
                  .outputMode("complete")
                  .format("memory")
                  .queryName("initDF")
                  .start())
        print("START reading")

        # sleepeing for 3 seconds then read
        time.sleep(30)
        res_df = spark.sql("select * from initDF")

        actual = res_df.collect()

        self.assertEqual(actual[0]['avg_price'], 35396.333333333336)
        self.assertEqual(actual[0]['total_records'], 3)
        self.assertEqual(actual[0]['total_sales'], 107480.3987779)

        self.assertEqual(actual[1]['avg_price'], 35356.0)
        self.assertEqual(actual[1]['total_records'], 2)
        self.assertEqual(actual[1]['total_sales'], 50701.00000000001)

        self.assertEqual(actual[2]['avg_price'], 35430.0)
        self.assertEqual(actual[2]['total_records'], 4)
        self.assertEqual(actual[2]['total_sales'], 128018.06796)

        self.assertEqual(actual[3]['avg_price'], 35375.0)
        self.assertEqual(actual[3]['total_records'], 1)
        self.assertEqual(actual[3]['total_sales'], 29999.549425)


if name == 'main':
    unittest.main()