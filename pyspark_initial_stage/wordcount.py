from pyspark import SparkContext

sc=SparkContext(master='local[*]')

txt=sc.textFile('hdfs:///files/dept.csv')

counts=txt.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a,b:a+b)

counts.saveAsTextFile('hdfs:///files/wordText.txt')
