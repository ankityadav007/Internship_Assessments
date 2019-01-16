from pyspark import SparkContext
#from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

sc=SparkContext(master='local[*]')
#spark=SparkSession(sc)
sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.avro.compression.codec","snappy")

data=sqlContext.read.parquet('hdfs://localhost:9000/files/dept')

#data.write.avro('hdfs://localhost:9000/files/dept_avro') 

data.write.format('com.databricks.spark.avro').save('hdfs://localhost:9000/files/dept_avro')

readData=spark.read.format('com.databricks.spark.avro').load('hdfs://localhost:9000/files/dept_avro')

readDdata.show()


#data.write.format("com.databricks.spark.avro").save('hdfs://localhost:9000/files/dept_avro')

