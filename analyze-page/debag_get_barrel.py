# coding: utf-8

import datetime
from pyspark import SparkConf, SparkContext
import debug_utils, load_local, parser


### test code: get_barrel ###
if __name__ == '__main__':
	sc = SparkContext("local", "Main Test")

	page_rdd = load_local.xml_to_dataframe(sc).rdd
	barrel_rdd = page_rdd.map(parser.get_barrel_)
	# print barrel_rdd.collect()
	print barrel_rdd
	print type(barrel_rdd)
	for w in barrel_rdd.collect():
		print w
	# print type(page_rdd.collect())
	# print page_rdd.collect()[0]
	# # print type(page_rdd.collect()[0])
	# print page_rdd.collect()[0]._id
	# print page_rdd.collect()[0]._title
	# print page_rdd.collect()[0]._url
	# print page_rdd.collect()[0].text
