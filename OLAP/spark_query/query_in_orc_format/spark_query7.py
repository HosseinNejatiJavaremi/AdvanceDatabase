from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="query7")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
customer = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/customer.orc")
lineitem = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/lineitem.orc")
nation = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/nation.orc")
orders = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/orders.orc")
supplier = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/supplier.orc")

from pyspark.sql import functions as F

getYear = lambda x: x[0: 4]
fun1 = lambda x, y: x * (1 - y)
nation_filter = nation.filter((nation.N_NAME == "IRAN") | (nation.N_NAME == "UNITED STATES"))
lineitem_filter = lineitem.filter((lineitem.L_SHIPDATE >= "1995-01-01") & (lineitem.L_SHIPDATE <= "1996-12-31"))

supplier_nation = nation_filter.join(supplier, nation_filter.N_NATIONKEY == supplier.S_NATIONKEY) \
    .join(lineitem_filter, supplier.S_SUPPKEY == lineitem_filter.L_SUPPKEY) \
    .select(nation_filter.N_NAME.alias("supp_nation"), lineitem_filter.L_ORDERKEY,
            lineitem_filter.L_EXTENDEDPRICE, lineitem_filter.L_DISCOUNT, lineitem_filter.L_SHIPDATE)

customer_nation = nation_filter.join(customer, nation_filter.N_NATIONKEY == customer.C_NATIONKEY) \
    .join(orders, customer.C_CUSTKEY == orders.O_CUSTKEY) \
    .select(nation_filter.N_NAME.alias("cust_nation"), orders.O_ORDERKEY)

query7 = customer_nation.join(supplier_nation, orders.O_ORDERKEY == supplier_nation.L_ORDERKEY) \
    .filter(((supplier_nation.supp_nation == "IRAN") & (customer_nation.cust_nation == "UNITED STATES"))
            | ((supplier_nation.supp_nation == "UNITED STATES") & (customer_nation.cust_nation == "IRAN"))) \
    .select(supplier_nation.supp_nation, customer_nation.cust_nation,
            getYear(lineitem_filter.L_SHIPDATE).alias("l_year"),
            fun1(lineitem_filter.L_EXTENDEDPRICE, lineitem_filter.L_DISCOUNT).alias("volume"))

query7 = query7.groupBy(supplier_nation.supp_nation, query7.cust_nation, query7.l_year) \
    .agg(F.sum("volume").alias("revenue")) \
    .sort(supplier_nation.supp_nation, query7.cust_nation, query7.l_year)

query7.show()
