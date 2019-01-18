from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="query8")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
customer = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/customer.orc")
lineitem = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/lineitem.orc")
nation = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/nation.orc")
orders = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/orders.orc")
part = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/part.orc")
region = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/region.orc")
supplier = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/supplier.orc")

from pyspark.sql import functions as F

getYear = lambda x: x[0: 4]
fun1 = lambda x, y: x * (1 - y)

region_filter = region.filter(region.R_NAME == "ASIA")
order_filter = orders.filter((orders.O_ORDERDATE <= "1996-12-31") & (orders.O_ORDERDATE >= "1995-01-01"))
part_filter = part.filter(part.P_TYPE == "ECONOMY POLISHED STEEL")

nation_supplier = nation.join(supplier, nation.N_NATIONKEY == supplier.S_NATIONKEY)

line = lineitem.select(lineitem.L_PARTKEY, lineitem.L_SUPPKEY, lineitem.L_ORDERKEY,
                       fun1(lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT).alias("volume")) \
    .join(part_filter, lineitem.L_PARTKEY == part_filter.P_PARTKEY) \
    .join(nation_supplier, lineitem.L_SUPPKEY == nation_supplier.S_SUPPKEY)

query8 = nation.join(region_filter, nation.N_REGIONKEY == region_filter.R_REGIONKEY) \
    .select(nation.N_NATIONKEY) \
    .join(customer, nation.N_NATIONKEY == customer.C_NATIONKEY) \
    .select(customer.C_CUSTKEY) \
    .join(order_filter, customer.C_CUSTKEY == order_filter.O_CUSTKEY) \
    .select(order_filter.O_ORDERKEY, order_filter.O_ORDERDATE) \
    .join(line, order_filter.O_ORDERKEY == line.L_ORDERKEY) \
    .select(getYear(order_filter.O_ORDERDATE).alias("o_year"), line.volume,
            F.when(nation.N_NAME == 'IRAN', line.volume).otherwise(0).alias("case_volume"))

query8 = query8.groupBy(query8.o_year) \
    .agg(F.sum(query8.case_volume) / F.sum(query8.volume)) \
    .sort(query8.o_year)

query8.show()
