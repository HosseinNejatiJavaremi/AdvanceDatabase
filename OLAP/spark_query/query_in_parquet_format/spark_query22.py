from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="query22")
sqlContext = SQLContext(sc)

customer = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/customer.parquet")
orders = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/orders.parquet")

from pyspark.sql import functions as F

customer_temp = customer.select(customer.C_ACCTBAL, customer.C_CUSTKEY,
                                customer.C_PHONE.substr(0, 2).alias("cntrycode"))

customer_filter = customer_temp.filter(customer_temp.cntrycode.isin(['13', '17', '21', '23', '31', '30', '41']))

customer_average = customer_filter.filter(customer_filter.C_ACCTBAL > 0.0) \
    .agg(F.avg(customer_filter.C_ACCTBAL).alias("avg_acctbal"))

query22 = orders.groupBy(orders.O_CUSTKEY) \
    .agg(orders.O_CUSTKEY).select(orders.O_CUSTKEY) \
    .join(customer_filter, orders.O_CUSTKEY == customer_filter.C_CUSTKEY, "right_outer") \
    .filter(orders.O_CUSTKEY.isNull()) \
    .join(customer_average) \
    .filter(customer_filter.C_ACCTBAL > customer_average.avg_acctbal) \
    .groupBy(customer_filter.cntrycode) \
    .agg(F.count(customer_filter.C_ACCTBAL).alias("numcust"),
         F.sum(customer_filter.C_ACCTBAL).alias("totacctbal")) \
    .sort(customer_filter.cntrycode)
query22.show()
