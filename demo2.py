from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import SparkSession

sc=SparkContext("local","demo2")
file=sc.textFile(r"C:\Users\SouvikChattopadhyay\Desktop\emp_hive_raw.csv")
print(file.first())
print(file.count())
print(file.collect())
print(type(file))
csv_rdd=file.map(lambda row: row.split(","))
csv_rdd1=csv_rdd.map(lambda r: Row(emp_id=int(r[0]),
    name=r[1],
    company=r[2],
    location=r[3],
    salary=r[4],
    dept_id=r[5]))

print(csv_rdd1.take(6))

sqlcontext=SQLContext(sc)
df=sqlcontext.createDataFrame(data=csv_rdd1)
df.show()
df_sql=df.registerTempTable("emp")
sql1_result=sqlcontext.sql("select * from emp where company='Wipro'")

for i in sql1_result.collect():
    print(i)