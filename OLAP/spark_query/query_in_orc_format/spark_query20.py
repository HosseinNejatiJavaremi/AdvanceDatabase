from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="query20")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
lineitem = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/lineitem.orc")
nation = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/nation.orc")
part = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/part.orc")
partsupp = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/partsupp.orc")
supplier = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/supplier.orc")

from pyspark.sql import functions as F

lineitem_filter = lineitem.filter((lineitem.L_SHIPDATE >= "1997-01-01") &
                                  (lineitem.L_SHIPDATE < "1998-01-01")) \
    .groupBy(lineitem.L_PARTKEY, lineitem.L_SUPPKEY) \
    .agg((F.sum(lineitem.L_QUANTITY) * 0.5).alias("sum_quantity"))

iran = nation.filter(nation.N_NAME == "IRAN")

iran_supplier = supplier.select(supplier.S_SUPPKEY, supplier.S_NAME,
                                supplier.S_NATIONKEY, supplier.S_ADDRESS) \
    .join(iran, supplier.S_NATIONKEY == iran.N_NATIONKEY)

query20 = part.filter(part.P_NAME.like("blue%")) \
    .select(part.P_PARTKEY).distinct() \
    .join(partsupp, part.P_PARTKEY == partsupp.PS_PARTKEY) \
    .join(lineitem_filter, (partsupp.PS_SUPPKEY == lineitem_filter.L_SUPPKEY) &
          (partsupp.PS_PARTKEY == lineitem_filter.L_PARTKEY)) \
    .filter(partsupp.PS_AVAILQTY > lineitem_filter.sum_quantity) \
    .select(partsupp.PS_SUPPKEY).distinct() \
    .join(iran_supplier, partsupp.PS_SUPPKEY == iran_supplier.S_SUPPKEY) \
    .select(iran_supplier.S_NAME, iran_supplier.S_ADDRESS) \
    .sort(iran_supplier.S_NAME)

query20.show()
