from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import broadcast



sc = SparkContext(master='local[2]')
spark = SparkSession(sc)




deptDF=spark.read.csv("/home/ankityadav/pyspark/dept.csv",header=True,inferSchema=True)

empDF=spark.read.csv("/home/ankityadav/pyspark/emp.csv",header=True,inferSchema=True)

deptEmpDF=deptDF.join(broadcast(empDF), deptDF.deptno == empDF.deptno).drop(empDF.deptno)

deptEmpDF.show()

print(deptEmpDF.filter((empDF['deptno'] >= '20') & (empDF['job'] == 'MANAGER') & (empDF['sal'] > '2000')).show())

print(empDF.select('job').distinct())

print(empDF.select('ename').show())

print(empDF.crosstab('empno','deptno').show())

print(deptEmpDF.groupBy(deptDF['deptno']).agg({'sal': 'max'}).show())

print(deptEmpDF.orderBy(deptEmpDF.sal.desc()).show())



newDeptEmpDF.show()

newDeptEmpDF = deptEmpDF.filter("deptno in (10, 20, 30)").groupBy("deptno").agg(func.min(deptEmpDF.sal).alias("min_sal"), func.max(deptEmpDF.sal).alias("max_sal"), func.sum(deptEmpDF.sal).alias("sum_sal"), func.avg(deptEmpDF.sal).alias("avg_sal"), func.count(deptEmpDF.sal).alias("count_sal"))

newDeptEmpDF.show()

finalDF=deptEmpDF.join(newDeptEmpDF,deptEmpDF.deptno==newDeptEmpDF.deptno, 'left_outer').drop(newDeptEmpDF.deptno)

finalDF.show()

finalDF.write.format('jdbc').options(url='jdbc:sqlserver://localhost:1433;databaseName=TestDB;user=SA;password=Ankit123',dbtable='sparkdata').save()

