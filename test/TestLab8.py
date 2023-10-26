import unittest

from pyspark.sql import SparkSession

from lab8util import get_total_price_and_sales

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
        df = spark.createDataFrame(prepare_df_for_aggregation(), schema=['eventTime', 'amount', 'price'])

        agg_df = get_total_price_and_sales(df)

        expected1 = [('2023-10-25 09:18:05', 39.5, 310.81)]
        expected2 = [('2023-10-25 09:17:05', 19.0, 68.38)]
        expected3 = [('2023-10-25 09:16:05', 52.60000000000001, 379.46)]

        actual = agg_df.collect()

        self.assertEqual(len(actual), 3)
        self.assertEqual([actual[0]], expected1)
        self.assertEqual([actual[1]], expected2)
        self.assertEqual([actual[2]], expected3)


if __name__ == '__main__':
    unittest.main()
