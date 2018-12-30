from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="Parquet2Orc")
sqlContext = SQLContext(sc)


sqlContext.setConf('spark.sql.orc.impl', 'native')

orders = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/orders.parquet")

orders.write.save("hdfs://namenode:8020/hossein-orc-data/orders.orc", mode='overwrite', format='orc')

orders_orc = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/orders.orc")
print(orders_orc.schema)
