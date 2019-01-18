from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="Parquet2Orc")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
supplier = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/supplier.parquet")

supplier.write.save("hdfs://namenode:8020/hossein-orc-data/supplier.orc", mode='overwrite', format='orc')

supplier_orc = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/supplier.orc")
print(supplier_orc.schema)
