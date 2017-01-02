# coding: utf-8

import datetime
from pyspark import SparkConf, SparkContext

import load_local, util, debug_utils


### test code: load_page ###
if __name__ == '__main__':
	sc = SparkContext("local", "Main Test")

	page_rdd = (load_local.mysql_to_dataframe(sc)
						  .rdd)
	# page_rdd.show()
	print page_rdd
	print type(page_rdd)
	print page_rdd.collect()
	print type(page_rdd.collect())
	print page_rdd.collect()[0]
	print type(page_rdd.collect()[0])
