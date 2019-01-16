import pyspark
import pymssql
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType

sc = SparkContext()
spark = SparkSession(sc)

fireDataDF=spark.read.csv("hdfs://localhost:9000/files/dept.csv",header=True,inferSchema=True)

'''Writing parquet'''
fireDataDF.write.parquet("/home/ankityadav/pyspark/firedata")

'''Reading parquet'''
fireDataDF=spark.read.parquet("/home/ankityadav/pyspark/firedata")

fireDataDF.show()
fireDataDF.write.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=Ankit123;useServerPrepStmts=false;rewriteBatchedStatements=true', dbtable='abcfire').save()


