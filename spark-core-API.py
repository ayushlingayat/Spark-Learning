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

words = ("big","Data","Is","SUPER","Intersting","BIG","data","IS","A","Trending","technology")

#it is a local collection it not something to do with spark now

# now I want to create a rdd means distributed collection across various machine

words_rdd = spark.sparkContext.parallelize(words)

words_normalized = words_rdd.map(lambda x : x.lower())

words_normalized.collect()

mapped_words = words_normalized.map(lambda x : (x,1))

aggregated_result = mapped_words.reduceByKey(lambda x,y : x+y)

aggregated_result.collect()

#Everything if we chaned this should also work okk see now

spark.sparkContext.parallelize(words).map(lambda x : x.lower()).map(lambda x : (x,1)).reduceByKey(lambda x,y : x+y).collect()

# chaining transformation

# result = spark. /
# sparkContext. /
# parallelize(words). /
# map(lambda x : x.lower()). /
# map(lambda x : (x,1)). /
# reduceByKey(lambda x,y : x+y)

# result.collect()






