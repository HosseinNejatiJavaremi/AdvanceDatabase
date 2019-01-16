from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="query9")
sqlContext = SQLContext(sc)

supplier = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/supplier.parquet")
lineitem = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/lineitem.parquet")
nation = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/nation.parquet")
orders = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/orders.parquet")
part = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/part.parquet")
partsupp = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/partsupp.parquet")

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
