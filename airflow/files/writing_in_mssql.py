from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)


deptEmpDF=spark.read.parquet("hdfs://localhost:9000/files/deptEmpDF")

deptEmpDF.show()
deptEmpDF.write.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=Ankit123;useServerPrepStmts=false;rewriteBatchedStatements=true', dbtable='fireAirflow').save()


