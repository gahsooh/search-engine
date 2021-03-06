# coding: utf-8

import json, sys, copy, datetime
from pyspark import SparkConf, SparkContext
from operator import add
import os

#import load_local as load
import load

import pagerank
import parser
import util


### test code ###
if __name__ == '__main__':
    sc = SparkContext(appName="Perse Page")

    page_rdd = sc.textFile(load.DATA_PATH).map(util.encode)
    page_rdd.map(lambda page: parser.parse_page(page)) \
            .filter(lambda (barrels, links): barrels is not None) \
            .filter(lambda (barrels, links): len(barrels['words']) > 20) \
            .map(lambda x: json.dumps(x)) \
            .saveAsTextFile(load.PARSED_PAGE_PATH)
