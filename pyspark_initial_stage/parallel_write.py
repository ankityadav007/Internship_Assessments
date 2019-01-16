import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc=SparkContext(master='local[*]')
spark=SparkSession(sc)

data = spark.read.parquet("hdfs://localhost:9000/files/firedata")

data.write.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=Ankit123', dbtable='fireAirflow',lowerBound=0,upperBound=100,numPartitions=8).save()

