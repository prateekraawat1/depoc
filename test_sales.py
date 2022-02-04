from unittest import TestCase
from sales import *
from pandas.testing import assert_frame_equal
import datetime
import pandas as pd

test_data = [("Asia", "China", "Baby Food", "Offline", "H", datetime.date(2010, 1, 3), 669865933, 9910.0
              , 255.28, 2529824.21, 951410.50),
             ("Europe", "Russia", "Household", "Offline", "L", datetime.date(2016, 6, 18), 669165444, 9995.0
              , 255.28, 2551523.60, 951490.50),
             ("Asia", "India", "Personal Care", "Offline", "C", datetime.date(2011, 8, 22), 669169922, 6078.0
              , 255.28, 1551591.11, 951780.50)]

region_test_data = [("Europe", 2551523.60),
                    ("Asia", 4081415.32)]

country_test_data = [("Europe", "Russia", "Household", "Offline", "L", datetime.date(2016, 6, 18), 669165444, 9995.0
                      , 255.28, 2551523.60, 951490.50)]

profit_test_data = [ ("Asia", "India", "Personal Care", "Offline", "C", datetime.date(2011, 8, 22), 669169922, 6078.0
                      , 255.28, 1551591.11, 951780.50)]

region_schema = StructType().add("Region", StringType(), True) \
    .add('Total Revenues', DoubleType(), True)


test_schema = StructType().add("Region", StringType(), True) \
    .add('Country', StringType(), True) \
    .add('Item Type', StringType(), True) \
    .add('Sales Channel', StringType(), True) \
    .add('Order Priority', StringType(), True) \
    .add('Order Date', DateType(), True) \
    .add('Order ID', LongType(), True) \
    .add('Units Sold', DoubleType(), True) \
    .add('Unit Price', DoubleType(), True) \
    .add('Total Revenue', DoubleType(), True) \
    .add('Total Profit', DoubleType(), True)


class TestSales(TestCase):
    def test_total_revenue_per_region(self):
        salesObj = Sales()
        testDF = salesObj.createDataframe(test_data, test_schema)

        revenueDF = salesObj.totalRevenuePerRegion(testDF)

        checkDF = salesObj.createDataframe(region_test_data, region_schema)

        revenueDF.show()
        checkDF.show()

        revenueDF = revenueDF.toPandas()
        checkDF = checkDF.toPandas()

        assert_frame_equal(revenueDF, checkDF)

    def test_top5countries(self):
        salesObj = Sales()
        testDF = salesObj.createDataframe(test_data, test_schema)

        testDF = salesObj.top5Countries(testDF, 'Household')

        checkDF = salesObj.createDataframe(country_test_data, test_schema)

        testDF.show()
        checkDF.show()

        testDF = testDF.toPandas()
        checkDF = checkDF.toPandas()

        assert_frame_equal(testDF, checkDF)


    def test_total_profit(self):
        salesObj = Sales()
        testDF = salesObj.createDataframe(test_data, test_schema)

        testDF = salesObj.totalProfit(testDF)

        checkDF = salesObj.createDataframe(profit_test_data, test_schema)

        testDF.show()
        checkDF.show()

        testDF = testDF.toPandas()
        checkDF = checkDF.toPandas()

        assert_frame_equal(testDF, checkDF)
