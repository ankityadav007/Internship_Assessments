from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc=SparkContext(master='local[*]')

spark=SparkSession(sc)

#data=spark.read.csv('hdfs://localhost:9000/files/dept.csv')

data = spark.read.format("CSV") .option("header","true").option("delimiter", ',').load('hdfs://localhost:9000/files/dept.csv')

#data = data.select('data.deptno','data.dname')
cols = data.columns

for col in cols:
	df2 = data.withColumnRenamed(col, col.replace(" ", ""))
	data=df2


data.write.format('json').save('hdfs://localhost:9000/files/dept.json')


readData = spark.read.json('hdfs://localhost:9000/files/dept.json')
readData.show()
