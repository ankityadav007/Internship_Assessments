from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import yaml
import yaycl,yaycl_crypt

sc = SparkContext(master='local')
spark = SparkSession(sc)


'''reading from HDFS and writing into  '''


filteredDF=spark.read.parquet("hdfs:///files/accident/filteredDF")

conf = yaycl.Config('/home/hduser/', crypt_key='my secret')

yaycl_crypt.decrypt_yaml(conf, 'test')


with open("/home/hduser/test.yaml", 'r') as ymlfile:
	cfg = yaml.load(ymlfile)

yaycl_crypt.encrypt_yaml(conf, 'test')


filteredDF.write.format(cfg['type'].get('conn_type')).options(url=cfg['url'].get('path')+cfg['url'].get('databaseName')+cfg['url'].get('user')+cfg['url'].get('password'), dbtable=cfg['dbtable'].get('name')).save()


newDF = spark.read.format(cfg['type'].get('conn_type')).options(url=cfg['url'].get('path')+cfg['url'].get('databaseName')+cfg['url'].get('user')+cfg['url'].get('password'), dbtable=cfg['dbtable'].get('name')).load()




'''writing into postgres '''


newDF.write.format(cfg['type'].get('conn_type')).options(url=cfg['url'].get('database_postgre'), dbtable=cfg['dbtable'].get('pname')).save()


fireDataDF.show()


