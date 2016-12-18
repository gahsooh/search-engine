# using: utf-8

from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *

### test code: read_xml ###
# command $ spark-submit --packages com.databricks:spark-xml_2.10:0.4.1 --master local read_xml.py 2> /dev/null
if __name__ == '__main__':
	sc = SparkContext("local", "Test Read XML")
	sqlContext = SQLContext(sc)

	df = (sqlContext.read
			.format('com.databricks.spark.xml')
			.options(rowTag='book')
			# .load('../data/full/raw/extracted/AA/wiki_00.xml'))
			# .load('books.xml'))
			.load('../data/min/raw/wiki_min.xml'))

	print df.printSchema()
	print df.show(5)
	print df.count()

	# (df.select("author", "_id").write
	# 	.format('com.databricks.spark.xml')
	# 	.options(rowTag='book', rootTag='books')
	# 	.save('newbooks.xml'))