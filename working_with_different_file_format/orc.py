from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc=SparkContext(master='local[*]')

spark=SparkSession(sc)

#data=spark.read.csv('hdfs://localhost:9000/files/dept.csv')

data = spark.read.format("CSV") .option("header","true").option("delimiter", ',').load('hdfs://localhost:9000/files/dept.csv')

data.write.orc('hdfs://localhost:9000/files/dept_orc')


readData=spark.read.orc('hdfs://localhost:9000/files/dept_orc')

readData.show()
