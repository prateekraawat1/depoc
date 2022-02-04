import unittest
from transactions import Transactions
from retail import Retail
from corporate import Corporate
from pyspark.sql import SparkSession
import PySparktest
from pyspark.sql.types import *
import pandas as pd

test_schema = StructType().add("Account_No", StringType(), True) \
    .add("DATE", StringType(), True) \
    .add("TRANSACTION_DETAILS", StringType(), True) \
    .add("CHQ_NO", IntegerType(), True) \
    .add("VALUE_DATE", StringType(), True) \
    .add("WITHDRAWAL_AMT", StringType(), True) \
    .add("DEPOSIT_AMT", StringType(), True) \
    .add("BALANCE_AMT", StringType(), True) \
    .add(".", StringType(), True)

test_schema_r = StructType().add("Account_No", StringType(), True) \
    .add("DATE", StringType(), True) \
    .add("TRANSACTION_DETAILS", StringType(), True) \
    .add("CHQ_NO", IntegerType(), True) \
    .add("VALUE_DATE", StringType(), True) \
    .add("WITHDRAWAL_AMT", StringType(), True) \
    .add("DEPOSIT_AMT", StringType(), True) \
    .add("BALANCE_AMT", StringType(), True) \
    .add("TransactionAmount", StringType(), True) \
    .add("TransactionType", StringType(), True) \
    .add("CustomerNames", StringType(), True)

test_df = [("1000423", "2017-12-23", "RTGS Common man", None, "2017-12-23", None, "20000", "180000", "."),
           ("4342000423", "2018-01-23", "RTGS Uncommon man", None, "2018-01-23", None, "40000", "190000", "."),
           ("4343004223", "2017-12-25", "RTGS UnCommon man", None, "2017-12-25", "21000", None, "220000", "."),
           ("1000621", "2019-12-23", "RTGS Common man", None, "2019-12-23", "10000", None, "100000", "."),
           ("1000822", "2020-06-23", "RTGS Common man", None, "2020-06-23", None, "80000", "21000", ".")]

test_retail_df = [
    ("4342000423", "2018-01-23", "RTGS Uncommon man", None, "2018-01-23", None, "40000", "190000", "40000", "CR", None),
    ("4343004223", "2017-12-25", "RTGS UnCommon man", None, "2017-12-25", "21000", None, "220000", "21000", "DR", None)]


class MyTestCase(unittest.TestCase):

    spark = SparkSession.builder.getOrCreate()

    def are_dataframe_equal(self, df1, df2):
        pdf1 = df1.toPandas()
        pdf2 = df2.toPandas()
        #TODO: add column check / use assertframeEquals
        if pdf1.equals(df2):
            return True
        else:
            return False

    @staticmethod
    def test_retail_constructor():
        r = Retail(schema=test_schema, dataframe=test_df)
        r.filterRetailTransactions()
        r.displayDF()
        assert (r.df.count() != 0)

    def test_corporate_constructor(self):
        c = Corporate(schema=test_schema, dataframe=test_df)
        c.displayDF()
        assert (c.df.count() != 0)

    def test_retail_tr(self):
        r = Retail(schema=test_schema, dataframe=test_df)
        r.filterRetailTransactions()
        r.displayDF()
        data = [
            ("4342000423", "2018-01-23", "RTGS Uncommon man", None, "2018-01-23", None, "40000", "190000", "40000",
             "CR", None),
            ("4343004223", "2017-12-25", "RTGS UnCommon man", None, "2017-12-25", "21000", None, "220000", "21000",
             "DR", None)]

        # test_retail_df = pd.DataFrame(data, columns=['Account_No', 'DATE', 'TRANSACTION_DETAILS', 'CHQ_NO',
        #                                              'VALUE_DATE', 'WITHDRAWAL_AMT', 'DEPOSIT_AMT', 'BALANCE_AMT',
        #                                              'TransactionAmount', 'TransactionType', 'CustomerNames'])

        columns=['Account_No', 'DATE', 'TRANSACTION_DETAILS', 'CHQ_NO',
                 'VALUE_DATE', 'WITHDRAWAL_AMT', 'DEPOSIT_AMT', 'BALANCE_AMT',
                 'TransactionAmount', 'TransactionType', 'CustomerNames']

        t_retail_df = self.spark.createDataFrame(data=data, schema=test_schema_r)
        return self.are_dataframe_equal(r.df, t_retail_df)


    def test_corporate_tr(self):
        c = Corporate(schema=test_schema, dataframe=test_df)
        c.displayDF()
        self.assertEqual(c.df.count(), 2)

    def test_drop_column(self):
        r = Retail(schema=test_schema, dataframe=test_df)
        df = r.dropColumn('VALUE_DATE')
        df.show()
        if 'VALUE_DATE' in df.columns:
            return False
        return True

    def test_corporate_only(self):
        c = Corporate(schema=test_schema, dataframe=test_df)
        test_data = [("1000423", "2017-12-23", "RTGS Common man", None, "2017-12-23", None, "20000", "180000", "20000", "CR", None),
                   ("1000621", "2019-12-23", "RTGS Common man", None, "2019-12-23", "10000", None, "100000", "10000", "DR", None),
                   ("1000822", "2020-06-23", "RTGS Common man", None, "2020-06-23", None, "80000", "21000", "80000", "CR", None)]

        c.addCustomerName()
        test_df_c = self.spark.createDataFrame(data=test_data, schema=test_schema_r)
        return self.are_dataframe_equal(test_df_c, c.df)

    def test_retail_agg_col(self):
        r = Retail(schema=test_schema, dataframe=test_df)
        r.filterRetailTransactions()
        r.addAggregateColumns()

        result_d = [("4342000423", "Jan 2018", "40000", None, None),
                    ("4342004223", "Dec 2017", None, "21000", None)]

        result_schema = StructType().add("Account_No", StringType(), True).add("MonthYear", StringType(), True) \
        .add("SumDepositPerMonth", StringType(), True).add("SumWithdrawalPerMonth", StringType(), True)


        pass




if __name__ == '__main__':
    unittest.main()
