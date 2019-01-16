from pyspark import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext()
spark = SparkSession(sc)

dept=spark.read.parquet("hdfs://localhost:9000/files/dept")

emp=spark.read.parquet("hdfs://localhost:9000/files/emp")

df1 = dept.alias('df1')
df2 = emp.alias('df2')

deptEmpDF=df1.join(df2, df1.deptno == df2.deptno).select('*')

drop_list = [c for c in df1.columns if c in df2.columns]
for col in drop_list:
    deptEmpDF = deptEmpDF.drop(df2[col])

deptEmpDF.write.parquet("hdfs://localhost:9000/files/deptEmpDF")

