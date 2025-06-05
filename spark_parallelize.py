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


words = ("big","Data","Is","SUPER","Intersting","BIG","data","IS","A","Trending","technology")

words_rdd = spark.sparkContext.parallelize(words)

words_rdd.getNumPartitions() #2

#to get default partition use this okk

spark.sparkContext.defaultMinPartition  #2

# we are getting 2 partion by using parallelize


orders_rdd = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")

mapped_rdd = orders_rdd.map(lambda x : (x.split(",")[3],1))

mapped_rdd.countByValue()