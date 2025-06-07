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

base_rdd = spark.sparkContext.textFile("/public/trendytech/orders/orders.csv")

mapped_rdd = base_rdd.map(lambda x: x.split(",")[3][1])

reduced_rdd = mapped_rdd.reduceByKey(lambda x,y: x+y)

reduced_rdd.collect()

spark.stop()
#it will be seen in the history server


#now for groupByKey

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

base_rdd = spark.sparkContext.textFile("/public/trendytech/orders/orders.csv")

mapped_rdd = base_rdd.map(lambda x: x.split(",")[3], x.split(",")[2])
#statud , id
# (CLOSED ,1159) => as an example ok

grouped_rdd = mapped_rdd.groupByKey()

result = grouped_rdd.map(lambda x : x[0],len(x[1]))
# (closed ,{11599,11502,39098})

result.collect()

spark.stop()

