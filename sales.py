from data import *

class Sales(Data):


dobj = Data()

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

df = dobj.readFile('salesData.csv', dschema)

df.show()

def to_date_(col, formats=("dd-MM-yyyy", "MM/dd/yyyy")):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_date(col, f) for f in formats])

df.withColumn('Order Date', to_date_('Order Date')).show()