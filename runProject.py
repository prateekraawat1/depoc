from customer import *
from sales import *

# Sales

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

# Customer


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

topBalance = cobj.topHighestBalance(dfi)
topBalance.show()