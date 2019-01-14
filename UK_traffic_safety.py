from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,broadcast
from pyspark.sql import functions as F
import pandas as pd
from matplotlib import pyplot as plt

sc = SparkContext(master='local')
spark = SparkSession(sc)


accident_info = spark.read.csv("hdfs:///files/accident/Accident_Information.csv",header=True,inferSchema=True)
vehicle_info = spark.read.csv("hdfs:///files/accident/Vehicle_Information.csv",header=True,inferSchema=True)


combinedDF = accident_info.join(vehicle_info,['Accident_Index']).drop(vehicle_info.Year)


print("----------------------------------------------------------------------------------------")

combinedDF.printSchema()

combinedDF.limit(5).show()

''' select , filter , groupBy , aggregations , orderBy '''

print(combinedDF.count())

combinedDF.filter(combinedDF['Sex_of_Driver'] == 'Male').show()

combinedDF.filter((combinedDF['Sex_of_Driver'] == 'Male') & (col('Age_Band_of_Driver').like('26%'))).limit(5).show()

combinedDF.select(accident_info.Year,combinedDF.Day_of_Week).groupBy('Year','Day_of_Week').agg(F.count(combinedDF.Day_of_Week)).show()

combinedDF.createOrReplaceTempView('accident')

usingSQL = spark.sql("select Year,Day_of_Week,count(Day_of_Week) from accident group By Year,Day_of_Week order by count(Day_of_Week)")

usingSQL.show()

usingSQL = spark.sql("select Year,Day_of_Week,count(Day_of_Week) from accident group By Year,Day_of_Week order by count(Day_of_Week)")





'''Number of Casualties as per years and displaying in Bar chart'''

dataDF=combinedDF.select(combinedDF.Year,combinedDF.Number_of_Casualties).groupBy('Year').agg({'Number_of_Casualties' : 'count'}).orderBy('Year').toPandas()

year = dataDF['Year'].tolist()

casualties = dataDF['count(Number_of_Casualties)'].tolist()

plt.bar(year,casualties,label="Example",color='b')
plt.legend()
plt.xlabel("Number of Casualties")
plt.ylabel("Year")
plt.title("Bar Graph")





'''Accidents categorized in Driver age criteria with equivalent accident numbers'''

combinedDF.select(combinedDF.Age_Band_of_Driver,'count(Age_Band_of_Driver)').groupBy('Age_Band_of_Driver').agg({'Age_Band_of_Driver' : 'count'}).show()





'''Number of Accidents categorized in journey purpose of driver with respect to each year'''

combinedDF.select(combinedDF.Year,combinedDF.Journey_Purpose_of_Driver).groupBy('Year','Journey_Purpose_of_Driver').agg({'Journey_Purpose_of_Driver' : 'count'}).orderBy('Year').show()





'''Number of Accidents categorized in accident situation,gednder for each year'''

combinedDF.select(combinedDF.Year,combinedDF.Vehicle_Manoeuvre,combinedDF.Sex_of_Driver).groupBy('Year','Vehicle_Manoeuvre','Sex_of_Driver').agg({'Vehicle_Manoeuvre' : 'count'}).orderBy('Year').show()




'''Creating two seprate dataframes and performing left outer join '''

vehicleDF= combinedDF.select(combinedDF.Accident_Index,combinedDF.Age_Band_of_Driver,combinedDF.Age_of_Vehicle,combinedDF.Journey_Purpose_of_Driver,combinedDF.make,combinedDF.model, combinedDF.Junction_Location).filter((col('Age_Band_of_Driver').like('26%')))

accidentDF= combinedDF.select(combinedDF.Accident_Index,combinedDF.Date,combinedDF.Day_of_Week,combinedDF.Road_Type,combinedDF.Speed_limit,combinedDF.Year).filter(combinedDF['Day_of_Week'] == 'Tuesday').orderBy('Year',ascending=True)

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




