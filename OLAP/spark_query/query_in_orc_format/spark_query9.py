from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="query9")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
lineitem = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/lineitem.orc")
nation = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/nation.orc")
orders = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/orders.orc")
part = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/part.orc")
partsupp = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/partsupp.orc")
supplier = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/supplier.orc")

from pyspark.sql import functions as F

getYear = lambda x: x[0: 4]
fun1 = lambda x, y, v, w: x * (1 - y) - (v * w)

lineitem_part = part.filter(part.P_NAME.contains("blue")) \
    .join(lineitem, part.P_PARTKEY == lineitem.L_PARTKEY)

nation_supplier = nation.join(supplier, nation.N_NATIONKEY == supplier.S_NATIONKEY)

query9 = lineitem_part.join(nation_supplier, lineitem_part.L_SUPPKEY == nation_supplier.S_SUPPKEY) \
    .join(partsupp, (lineitem_part.L_SUPPKEY == partsupp.PS_SUPPKEY)
          & (lineitem_part.L_PARTKEY == partsupp.PS_PARTKEY)) \
    .join(orders, lineitem_part.L_ORDERKEY == orders.O_ORDERKEY) \
    .select(nation_supplier.N_NAME, getYear(orders.O_ORDERDATE).alias("o_year"),
            fun1(lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT,
                 partsupp.PS_SUPPLYCOST, lineitem_part.L_QUANTITY).alias("amount"))

query9 = query9.groupBy(query9.N_NAME, query9.o_year) \
    .agg(F.sum(query9.amount)) \
    .sort(query9.N_NAME, query9.o_year.desc())

query9.show()
