import unittest

from pyspark.sql import SparkSession

from lab8util import get_total_price_and_sales, get_json_schema

spark = SparkSession.builder.appName("streaming_app").master('local[*]').getOrCreate()


def prepare_df_for_aggregation():
    df = [('2023-10-25 09:18:05', 10.3, 12.1), ('2023-10-25 09:18:05', 8.4, 15.6), ('2023-10-25 09:18:05', 4.3, 6.3),
          ('2023-10-25 09:18:05', 5.1, 5.5),
          ('2023-10-25 09:17:05', 2.5, 2.6), ('2023-10-25 09:17:05', 3.4, 6.2), ('2023-10-25 09:17:05', 4.0, 10.2),
          ('2023-10-25 09:16:05', 6.1, 12.8), ('2023-10-25 09:16:05', 3.3, 9.4), ('2023-10-25 09:16:05', 10.5, 15.6),
          ('2023-10-25 09:16:05', 7.2, 14.8)]
    return df


class TestMySparkFunctionsForTask1(unittest.TestCase):

    def test_get_total_sum_and_prices(self):
        df = spark.createDataFrame(prepare_df_for_aggregation(), schema=['event_time', 'amount', 'price'])

        agg_df = get_total_price_and_sales(df)

        expected1 = [(9.875, 4, 310.81)]
        expected2 = [(6.333333333333333, 3, 68.38)]
        expected3 = [(13.150000000000002, 4, 379.46)]

        actual = agg_df.collect()

        self.assertEqual(len(actual), 3)
        self.assertEqual([actual[0][1:]], expected1)
        self.assertEqual([actual[1][1:]], expected2)
        self.assertEqual([actual[2][1:]], expected3)

    def test_parse_json(self):
        df = spark.read.schema(get_json_schema()).json("data.json")
        actual = [row.asDict() for row in df.collect()]

        self.assertEqual(actual[0]['event'], 'order_deleted')
        self.assertEqual(actual[0]['data']['id'], '1676246737752066')
        self.assertEqual(actual[0]['data']['datetime'], '1698074907')
        self.assertEqual(actual[0]['data']['amount'], 0.05296822)
        self.assertEqual(actual[0]['data']['price'], 30814.0)


if __name__ == '__main__':
    unittest.main()
