from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query1")
sqlContext = SQLContext(sc)

lineitem = spark.read.parquet("hdfs://namenode:8020/hossein-parquet-data/lineitem.parquet")
# lineitem = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/lineitem.orc")
# lineitem = spark.read.format("com.databricks.spark.avro").load("hdfs://namenode:8020/hossein-avro-data/lineitem.avro")

from datetime import datetime
from datetime import timedelta
from pyspark.sql import functions as F

old_time = '1998-12-01'
new_time = datetime.strptime(old_time, '%Y-%m-%d') - timedelta(days=120)
new_time = str(new_time.date())
fun1 = lambda x, y: x * (1 - y)
fun2 = lambda x, y, z: x * (1 - y) * (1 + z)

query1 = lineitem.filter(lineitem.L_SHIPDATE <= new_time) \
    .groupBy(lineitem.L_RETURNFLAG, lineitem.L_LINESTATUS) \
    .agg(F.sum(lineitem.L_QUANTITY).alias('sum_qty'),
         F.sum(lineitem.L_EXTENDEDPRICE).alias('sum_base_price'),
         F.sum(fun1(lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT)).alias('sum_disc_price'),
         F.sum(fun2(lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT, lineitem.L_TAX).alias('sum_charge')),
         F.avg(lineitem.L_QUANTITY).alias('avg_qty'),
         F.avg(lineitem.L_EXTENDEDPRICE).alias('avg_price'),
         F.avg(lineitem.L_DISCOUNT).alias('avg_disc'),
         F.count('*').alias('count_order')) \
    .sort(lineitem.L_RETURNFLAG, lineitem.L_LINESTATUS)
query1.show()
