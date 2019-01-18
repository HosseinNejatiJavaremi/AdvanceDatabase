from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="query11")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
nation = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/nation.orc")
partsupp = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/partsupp.orc")
supplier = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/supplier.orc")

from pyspark.sql import functions as F

fun1 = lambda x, y: x * y
fun2 = lambda x: x * 0.000001

iran = nation.filter(nation.N_NAME == "IRAN") \
    .join(supplier, nation.N_NATIONKEY == supplier.S_NATIONKEY) \
    .select(supplier.S_SUPPKEY) \
    .join(partsupp, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY) \
    .select(partsupp.PS_PARTKEY, fun1(partsupp.PS_SUPPLYCOST, partsupp.PS_AVAILQTY).alias("value"))

total = iran.agg(F.sum("value").alias("total_value"))

query11 = iran.groupBy(partsupp.PS_PARTKEY).agg(F.sum("value").alias("group_value"))

query11 = query11.join(total, query11.group_value > fun2(total.total_value)) \
    .sort(query11.group_value.desc())
query11.show()
