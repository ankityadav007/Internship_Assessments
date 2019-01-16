from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

dept=spark.read.csv("hdfs://localhost:9000/files/dept.csv",header=True,inferSchema=True)


emp=spark.read.csv("hdfs://localhost:9000/files/emp.csv",header=True,inferSchema=True)


dept.write.parquet("hdfs://localhost:9000/files/dept")

emp.write.parquet("hdfs://localhost:9000/files/emp")


