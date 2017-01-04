# coding: UTF-8

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

WORD_LIST_PATH = "../data/min/word_list/"
WORD_SUGGEST_PATH = "../data/min/word_suggest/"
DOCUMENTS_PATH = "../data/min/documents/"
INVERTED_INDEX_PATH = "../data/min/inverted_index/"
PARSED_PAGE_PATH = "../data/min/parsed_page/"
DATA_PATH_MIN_JSON = "/Users/suganuma/github/search-engine/data/min/raw/page_data_1.txt"
DATA_PATH_MIN_XML = "/Users/suganuma/github/search-engine/data/min/raw/page_data_2.xml"
DATA_PATH_FULL_XML = "/Users/suganuma/github/search-engine/data/arrange/raw/page"


def loadPage(sc):
	return sc.textFile(DATA_PATH_MIN_JSON)

def pagesToDataframe(sc):
	sqlContext = SQLContext(sc)

	# Input: XML file
	xml_format = 'com.databricks.spark.xml'
	xml_root_tag = 'doc'
	xml_custom_schema = StructType([
		StructField("_id", StringType(), True),
		StructField("_title", StringType(), True),
		StructField("_url", StringType(), True),
		StructField("text", StringType(), True)])

	df = (sqlContext.read
			.format(xml_format)
			.option("charset", "UTF-8")
			.options(rowTag=xml_root_tag)
			.load(DATA_PATH_MIN_XML, schema = xml_custom_schema))

	return df

def pagelinksToDataframe(sc):
	sqlContext = SQLContext(sc)

	# Input: MySQL
	df = (sqlContext.read
			.format("jdbc")
			.option("url", "jdbc:mysql://localhost:3306/search_engine")
			.option("user", "root")
			.option("password", "")
			.option("dbtable", "debug_pagelinks")
			.option("driver", "com.mysql.jdbc.Driver")
			.load())

	return df


if __name__ == '__main__':
	sc = SparkContext(appName="Load Test")
	page_rdd = load_page(sc)
	print page_rdd.count()
	for b in page_rdd.collect():
		print b.encode("utf-8")

