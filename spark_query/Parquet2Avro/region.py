# pyspark  --executor-memory 3g --num-executors 12 --packages com.databricks:spark-avro_2.11:4.0.0

from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="Parquet2Avro")
sqlContext = SQLContext(sc)

# sqlContext.setConf('spark.driver.extraClassPath', '/usr/spark-2.3.0/jars/avro-1.8.2.jar')
# sqlContext.setConf('spark.executor.extraClassPath', '/usr/spark-2.3.0/jars/avro-1.8.2.jar')

region = spark.read.format('parquet').load("hdfs://namenode:8020/hossein-parquet-data/region.parquet")

region.write.format("com.databricks.spark.avro").mode('overwrite') \
    .save("hdfs://namenode:8020/hossein-avro-data/region.avro")

region_avro = spark.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/hossein-avro-data/region.avro")

print(region_avro.schema)
