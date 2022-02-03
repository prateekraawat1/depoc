from data import *


class Sales(Data):
    def __init__(self):
        super(Sales, self).__init__()

    def totalRevenuePerRegion(self, df):
        """
        Method to calculate Total Revenue by Regions
        :param df:
        :return:
        """
        newdf = df.groupBy('Region').agg(sum('Total Revenue').alias("Total Revenues"))
        return newdf

    def top5Countries(self, df, item):
        """
        Method to calculate Top 5 Countries by Item Type and Units Sold
        :param df:
        :param item: Item Type
        :return:
        """
        newdf = df.filter(df["Item Type"] == item)
        newdf = newdf.orderBy(df['Units Sold']).limit(5)
        return newdf

    def totalProfit(self, df):
        """
        Method to calculate total profit for a Region and within a date range
        :param df:
        :return:
        """
        # TODO: Remove hardcoded Region and date range

        newdf = df.filter(df['Region'] == "Asia").filter(df['Order Date'].between('2011-01-01', '2015-12-31')).orderBy(
            df['Order Date'])
        return newdf


sobj = Sales()

dschema = StructType().add("Region", StringType(), True) \
    .add('Country', StringType(), True) \
    .add('Item Type', StringType(), True) \
    .add('Sales Channel', StringType(), True) \
    .add('Order Priority', StringType(), True) \
    .add('Order Date', StringType(), True) \
    .add('Order ID', LongType(), True) \
    .add('Units Sold', DoubleType(), True) \
    .add('Unit Price', DoubleType(), True) \
    .add('Total Revenue', DoubleType(), True) \
    .add('Total Profit', DoubleType(), True)

df = sobj.readFile('salesData.csv', dschema)

df.show()


def to_date_(col, formats=("dd-MM-yyyy", "MM/dd/yyyy")):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_date(col, f) for f in formats])


df = df.withColumn('Order Date', to_date_('Order Date'))

rev = sobj.totalRevenuePerRegion(df)

rev.show()

top5 = sobj.top5Countries(df, "Household")

top5.show()

tp = sobj.totalProfit(df)

tp.show()
