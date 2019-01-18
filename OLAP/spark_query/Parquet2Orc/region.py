from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="Parquet2Orc")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
region = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/region.parquet")

region.write.save("hdfs://namenode:8020/hossein-orc-data/region.orc", mode='overwrite', format='orc')

region_orc = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/region.orc")
print(region_orc.schema)
region_orc.first()
