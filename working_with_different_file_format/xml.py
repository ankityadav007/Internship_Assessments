from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc=SparkContext(master='local[*]')
spark=SparkSession(sc)


data = spark.read.format("CSV").option("header","true").option("delimiter", ',').load('hdfs://localhost:9000/files/dept.csv')


data.write.format('com.databricks.spark.xml').save('hdfs://localhost:9000/files/dept_xml')

readData=spark.read.format('com.databricks.spark.xml').load('hdfs://localhost:9000/files/dept_xml')

readData.show()
