from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="Parquet2Orc")
sqlContext = SQLContext(sc)


sqlContext.setConf('spark.sql.orc.impl', 'native')
partsupp = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/partsupp.parquet")

partsupp.write.save("hdfs://namenode:8020/hossein-orc-data/partsupp.orc", mode='overwrite', format='orc')

partsupp_orc = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/partsupp.orc")
print(partsupp_orc.schema)
