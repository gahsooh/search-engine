# coding: utf-8

import datetime
from pyspark import SparkConf, SparkContext

import load_local, util, debug_utils


### test code: load_page ###
if __name__ == '__main__':
	sc = SparkContext("local", "Main Test")

	page_rdd = (load_local.xml_to_rdd(sc)
						  .map(util.encode))
	print page_rdd
	print type(page_rdd)
	print page_rdd.collect()
	# debug_utils.show_rdd(page_rdd)
