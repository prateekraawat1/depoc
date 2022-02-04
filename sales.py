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
        self.mylogger.info("Calculate total revenue by Regions")
        newdf = df.groupBy('Region').agg(sum('Total Revenue').alias("Total Revenues"))
        return newdf

    def top5Countries(self, df, item):
        """
        Method to calculate Top 5 Countries by Item Type and Units Sold
        :param df:
        :param item: Item Type
        :return:
        """
        self.mylogger.info("calculate Top 5 Countries by Item Type and Units Sold")
        newdf = df.filter(df["Item Type"] == item)
        newdf = newdf.orderBy(newdf['Units Sold']).limit(5)
        return newdf

    def totalProfit(self, df):
        """
        Method to calculate total profit for a Region and within a date range
        :param df:
        :return:
        """
        # TODO: Remove hardcoded Region and date range

        self.mylogger.info("Calculating total profit by region")
        newdf = df.filter(df['Region'] == "Asia").filter(df['Order Date'].between('2011-01-01', '2015-12-31')).orderBy(
            df['Order Date'])
        return newdf


