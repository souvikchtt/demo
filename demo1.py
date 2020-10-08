from pyspark import SparkContext
from pyspark.sql import SparkSession

sc=SparkContext("local","demo")
word=sc.parallelize(["hi","hello"])
print(word.count())

appName = "Scala Parquet Example"
master = "local"

spark = SparkSession.builder.appName(appName).master(master).getOrCreate()

df=spark.read.parquet("userdata1.parquet")
df.show()
print(df.count())
df.printSchema()


df.write.json("userdata1_json")

