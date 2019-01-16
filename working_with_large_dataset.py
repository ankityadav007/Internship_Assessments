import pyspark
import pymssql
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType

sc = SparkContext(master='local[2]')
spark = SparkSession(sc)

fireSchema = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),
                     StructField('CallDate', StringType(), True),
                     StructField('WatchDate', StringType(), True),
                     StructField('ReceivedDtTm', StringType(), True),
                     StructField('EntryDtTm', StringType(), True),
                     StructField('DispatchDtTm', StringType(), True),
                     StructField('ResponseDtTm', StringType(), True),
                     StructField('OnSceneDtTm', StringType(), True),
                     StructField('TransportDtTm', StringType(), True),
                     StructField('HospitalDtTm', StringType(), True),
                     StructField('CallFinalDisposition', StringType(), True),
                     StructField('AvailableDtTm', StringType(), True),
                     StructField('Address', StringType(), True),
                     StructField('City', StringType(), True),
                     StructField('ZipcodeofIncident', IntegerType(), True),
                     StructField('Battalion', StringType(), True),
                     StructField('StationArea', StringType(), True),
                     StructField('Box', StringType(), True),
                     StructField('OriginalPriority', StringType(), True),
                     StructField('Priority', StringType(), True),
                     StructField('FinalPriority', IntegerType(), True),
                     StructField('ALSUnit', BooleanType(), True),
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumberofAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('Unitsequenceincalldispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('NeighborhoodDistrict', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True)])

fireServiceCalldDF=spark.read.csv("/home/ankityadav/Downloads/Fire_Department_Calls_for_Service.csv",header=True,schema=fireSchema)

incidentsDF=spark.read.csv("/home/ankityadav/Downloads/Fire_Incidents.csv",header=True,inferSchema=True)

df1 = fireServiceCalldDF.alias('df1')
df2 = incidentsDF.alias('df2')

fireDataDF=df1.join(df2, df1.IncidentNumber == df2.IncidentNumber).select('*')

drop_list = ['IncidentNumber','Address','CallNumber','City','SupervisorDistrict','Location','Battalion','StationArea','Box','NumberofAlarms','NeighborhoodDistrict']

drop_list = [c for c in df1.columns if c in df2.columns]
for col in drop_list:
    fireDataDF = fireDataDF.drop(df2[col])

fireDataDF.show()

fireDataDF.write.parquet("/home/ankityadav/pyspark/firedata")

fireDataDF.write.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=Ankit123',dbtable='fireRecord').save()












