spark usecase 1 - orders data

from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()
spark = SparkSession. \
    builder. \
    config('spark.ui.port','0'). \
    config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

spark

#Count the orders in each category

orders_rdd = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")

orders_rdd.take(5)

mapped_rdd = orders_rdd.map(lambda x : (x.split(",")[3],1))

mapped_rdd.take(10)

mapped_rdd = orders_rdd.map(lambda x : (x.split(",")[3],1))

mapped_rdd.take(10)

reduced_rdd = mapped_rdd.reduceByKey(lambda x,y : x+y)

reduced_rdd.collect()

reduce_sorted = reduced_rdd.sortBy(lambda x : x[1])

orders_rdd.map(lambda x : (x.split(",")[2],1))


#Find the premium customers (Top 10 who placed most number of orders)

customers_mapped = orders_rdd.map(lambda x : (x.split(",")[2],1))

customers_mapped.take(5)

customer_aggregated = customers_mapped.reduceByKey(lambda x,y : x+y)

customer_aggregated.take(20)

customer_sorted = customer_aggregated.sortBy(lambda x : x[1],False)

customer_sorted.take(20)

#Distinct count of cutomers who placed atleast one order

distinct_customer = orders_rdd.map(lambda x : x.split(",")[2]).distinct()

distinct_customer.count() #This will show the count then

orders_rdd.count() #yeeh pura show karega kitne total the okk

#which customer has maximum number of closed orders

filter_order = orders_rdd.filter(lambda x : (x.split(",")[3] == 'CLOSED'))

filter_order.take(20)

filtered_map = filter_order.map(lambda x : x.split(",")[2],1)

filtered_map.take(20)

filtered_aggregate = filtered_map.reduceByKey(lambda x,y : x+y)

filtered_aggregate.take(20)

filtered_sorted = filtered_aggregate.sortBy(lambda x : x[1], False)

filtered_sorted.take(10)

#what have we covered here
map
reduce
reduceByKey
