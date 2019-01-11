from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import yaml
import yaycl,yaycl_crypt

sc = SparkContext(master='local')
spark = SparkSession(sc)


filteredDF = spark.write.partitionBy('Year').parquet("hdfs:///files/accident/filteredDF")

conf = yaycl.Config('/home/ankityadav/Assessment/1st/yaml/', crypt_key='/home/ankityadav/Assessment/1st/yaml/key.txt')

yaycl_crypt.decrypt_yaml(conf, 'test')

with open("/home/ankityadav/Assessment/1st/yaml/test.yaml", 'r') as ymlfile:
	cfg = yaml.load(ymlfile)

yaycl_crypt.encrypt_yaml(conf, 'test')

filteredDF.format(cfg['type'].get('conn_type')).options(url=cfg['url'].get('path')+cfg['url'].get('databaseName')+cfg['url'].get('user')+cfg['url'].get('password'), dbtable=cfg['dbtable'].get('name')).save()


empDF.write.format(cfg['type'].get('conn_type')).options((url=cfg['url'].get('path'), dbtable=cfg['dbtable']).save()


fireDataDF.show()





