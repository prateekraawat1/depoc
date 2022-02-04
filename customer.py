from data import *

class Customer(Data):
    def __init__(self):
        super(Customer, self).__init__()

    def joinDF(self, df1, df2, jointype):
        """
        Method to join two dataframes
        :param df1:
        :param df2:
        :param jointype:
        :return:
        """
        self.mylogger.info(f"Joining dataframes using {jointype}")
        newdf = df1.join(df2, on=['customerID'], how=jointype)

        return newdf

    def totalAccountsAssociated(self, df):
        """
        Method to find total accounts associated with a customer
        :param df:
        :return:
        """
        self.mylogger.info("Finding total accounts associated with a customer")
        newdf = df.groupBy(df['customerId'], df['forename'], df['surname']).count()
        return newdf

    def duplicateAcconts(self, df):
        formatteddf = self.totalAccountsAssociated(df)
        newdf = formatteddf.withColumnRenamed('count', "NoOfAccounts")
        newdf = newdf.filter(newdf['NoOfAccounts'] > 1)
        return newdf

    def topHighestBalance(self, df):
        """
        Method to find top 5 customers with highest balance
        :param df:
        :return:
        """
        self.mylogger.info("Finding top 5 customers with highest balance")
        return df.orderBy(df['balance'].desc()).limit(5)
