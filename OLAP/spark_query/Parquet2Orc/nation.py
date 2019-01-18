from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="Parquet2Orc")
sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.orc.impl', 'native')

nation = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/nation.parquet")

nation.write.save("hdfs://namenode:8020/hossein-orc-data/nation.orc", mode='overwrite', format='orc')

nation_orc = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/nation.orc")
print(nation_orc.schema)
