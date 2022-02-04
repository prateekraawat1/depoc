from pyspark.sql import SparkSession
import logging


class Data:
    def __init__(self):
        self.df = None
        self.spark = SparkSession \
            .builder \
            .appName("PySpark POC") \
            .getOrCreate()

        self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

        self.logFormat = "%(levelname)s %(asctime)s - %(message)s"

        logging.basicConfig(filename='logfile.log',
                            filemode='w',
                            format=self.logFormat,
                            level=logging.INFO)

        self.mylogger = logging.getLogger()

    def readFile(self, filename, fileschema):
        try:
            self.mylogger.info("Reading file")
            dataf = self.spark.read.schema(fileschema).csv(filename, header='true', sep=';')

            return dataf
        except:
            self.mylogger.error('Failed to read CSV file')
            print('Failed to read CSV file')

    def readFileWOSchema(self, filename):
        try:
            self.mylogger.info("Reading file")
            dataf = self.spark.read.csv(filename, header='true')

            return dataf
        except:
            self.mylogger.error('Failed to read CSV file')
            print('Failed to read CSV file')

    def saveFile(self, dataframe, filename):
        try:
            self.mylogger.info(f'Writing dataframe: {dataframe} to file {filename}')
            dataframe.coalesce(1).write.mode('overwrite').option('header', 'true').csv(filename)

        except:
            self.mylogger.error('Failed to write file')
            print('Failed to save file')

    def createDataframe(self, data, schema):
        return self.spark.createDataFrame(data=data, schema=schema)
