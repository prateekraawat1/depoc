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


cobj = Customer()

df1 = cobj.readFileWOSchema('account_data.csv')
df2 = cobj.readFileWOSchema('customer_data.csv')
# df = cobj.joinDF(df1, df2, 'outer')
dfi = cobj.joinDF(df1, df2, 'inner')
# df.orderBy(df['customerId']).show(50)
dfi.show()

cobj.saveFile(dfi, "ij.csv")
# cobj.saveFile(df, "oj.csv")

taccounts = cobj.totalAccountsAssociated(dfi)
taccounts.show(20)

dup = cobj.duplicateAcconts(dfi)
dup.show()
