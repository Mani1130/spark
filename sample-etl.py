from pyspark.sql.context import SQLContext
from pyspark.sql import SparkSession

warehouseLocation = 's3a://s3-phc-poc-02-sample-etl/'
spark = SparkSession.builder.appName("Test").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("INFO")
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')
sc._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider',
                                  'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')

#file = str('s3a://s3-phc-poc-02-sample-etl/data/height_weight.csv')
#rdd= spark.textFile('file')

#a=spark.sparkContext.textFile("s3a://s3-phc-poc-02-sample-etl/2020/08/04/06:19/Raw/criteo/day_1.gz")

#data_df = spark.read.options(header='True',inferSchema='True').csv(file)

data_df = spark.read.csv("s3a://s3-phc-poc-02-sample-etl/2020/08/*.gz") 
print('10 rows to display:')
data_df.show(10)


data_df.rdd.getNumPartitions()
data_df = data_df.repartition(200)
count=data_df.count()
print('Number of rows: ',count)


