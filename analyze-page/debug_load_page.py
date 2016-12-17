# coding: utf-8

import datetime
from pyspark import SparkConf, SparkContext

import load_local, util, debug_utils


### test code: load_page ###
if __name__ == '__main__':
	sc = SparkContext("local", "Main Test")

	print datetime.datetime.today()

	page_rdd = (load_local.load_page(sc)
						  .map(util.encode))

	print datetime.datetime.today()

	debug_utils.show_rdd(page_rdd)
