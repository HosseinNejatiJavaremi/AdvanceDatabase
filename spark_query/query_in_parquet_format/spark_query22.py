from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query22")
sqlContext = SQLContext(sc)

customer = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/customer.parquet")

custsale = customer.select(customer.C_PHONE.substr(1, 2).alias('cntrycode'), customer.C_ACCTBAL)
