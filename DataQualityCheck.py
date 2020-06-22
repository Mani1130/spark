from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
import pandas as pd
import com.crealytics.spark.excel as excel
import configparser

#Spark session initialisation and variable declaration
spark = SparkSession.builder.appName("QualityCheck").getOrCreate()
sqlContext = SQLContext(spark)
#Reading configs
config = configparser.ConfigParser()
config.read(r'config.ini')
inputFile = config.get('paths','inputFileLocation')

#Source file
df = pd.read_excel(str(inputFile)+'\HCPC2020_ANWEB_w_disclaimer.xls',inferSchema='')
print(df.dtypes)

spark.read.format(excel).load(str(inputFile)+'\HCPC2020_ANWEB_w_disclaimer.xls')
    #.option("dataAddress", "'HCPC2020_ANWEB_w_disclaimer.xls'") \
    #.load(inputFile)

#SourceFileDf = spark.createDataFrame(df)


SourceFileDf.show()