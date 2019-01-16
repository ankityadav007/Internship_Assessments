from pyspark import SparkContext
from pyspark.sql.session import SparkSession



sc = SparkContext(master='local[*]')
spark = SparkSession(sc)


finalDF=spark.read.parquet("hdfs:///files/emp")

finalDF.show()

finalDF.write.partitionBy("job").parquet("hdfs:///files/newEmp")

finalDF.repartition("job").write.partitionBy("job").parquet("/home/ankityadav/pyspark/newFinalDF")

finalDF=spark.read.parquet("hdfs:///files/newEmp/job=CLERK").show()

