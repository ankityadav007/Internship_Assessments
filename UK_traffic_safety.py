from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,broadcast
from pyspark.sql import functions as F

sc = SparkContext(master='local')
spark = SparkSession(sc)

accident_info = spark.read.csv("hdfs:///files/accident/Accident_Information.csv",header=True,inferSchema=True)
vehicle_info = spark.read.csv("hdfs:///files/accident/Vehicle_Information.csv",header=True,inferSchema=True)


combinedDF = accident_info.join(vehicle_info,['Accident_Index']).drop(vehicle_info.Year)


print("----------------------------------------------------------------------------------------")

combinedDF.printSchema()

combinedDF.limit(5).show()

print(combinedDF.count())

combinedDF.filter(combinedDF['Sex_of_Driver'] == 'Male').show()

combinedDF.filter((combinedDF['Sex_of_Driver'] == 'Male') & (col('Age_Band_of_Driver').like('26%'))).limit(5).show()

combinedDF.select(accident_info.Year,combinedDF.Day_of_Week).groupBy('Year','Day_of_Week').agg(F.count(combinedDF.Day_of_Week)).show()

combinedDF.createOrReplaceTempView('accident')

usingSQL = spark.sql("select Year,Day_of_Week,count(Day_of_Week) from accident group By Year,Day_of_Week order by count(Day_of_Week)")

usingSQL.show()

usingSQL = spark.sql("select Year,Day_of_Week,count(Day_of_Week) from accident group By Year,Day_of_Week order by count(Day_of_Week)")

''' select , filter , groupBy , aggregations , orderBy '''

print('Total No. of Accidents in Year 2016:----')
combinedDF.select(accident_info.Accident_Index,combinedDF.Year).groupBy('Year').agg({'Accident_Index' : 'count'}).show()

vehicleDF= combinedDF.select(combinedDF.Accident_Index,combinedDF.Age_Band_of_Driver,combinedDF.Age_of_Vehicle,combinedDF.Journey_Purpose_of_Driver,combinedDF.make,combinedDF.model, combinedDF.Junction_Location).filter((col('Age_Band_of_Driver').like('26%')))

accidentDF= combinedDF.select(combinedDF.Accident_Index,combinedDF.Date,combinedDF.Day_of_Week,combinedDF.Road_Type,combinedDF.Speed_limit,combinedDF.Year).filter(combinedDF['Day_of_Week'] == 'Tuesday').orderBy('Year',ascending=True)


''' Left Outer JOIN '''

filteredDF = accidentDF.join(vehicleDF,accidentDF.Accident_Index == vehicleDF.Accident_Index, "left_outer").drop(accidentDF.Accident_Index)

''' Broadcast join '''

broadcastDF = combinedDF.join(filteredDF,['Accident_Index']).drop(filteredDF.Year)

df1 = combinedDF.alias('df1')
df2 = filteredDF.alias('df2')

drop_list = [c for c in df1.columns if c in df2.columns]
for col in drop_list:
	broadcastDF = broadcastDF.drop(df2[col])

broadcastDF.show()

''' Using partitionBy and rePartitionBy in parquet '''

filteredDF.write.partitionBy('Year').parquet("hdfs:///files/accident/filteredDF")

#filteredDF.repartition('Day_of_Week').write.partitionBy('Year').parquet("hdfs:///files/accident/filteredDF")

''' python generator object using yield '''

def my_func():
	headers = [column for column in filteredDF.columns]
	for column in headers:
		print('These are the fiels of filteredDF')
		yield column

generator=my_func()
next(generator)

print("----------------------------------------------------------------------------------------")




