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
    sc = SparkContext(appName="Documents")

    print datetime.datetime.today()
    
    print 'start load'
    barrels_rdd = sc.textFile(load.PARSED_PAGE_PATH).map(util.encode).keys()

    print 'start store document list'
    barrels_rdd.map(lambda x: (x['url'], (x['title'], x['content']))) \
               .distinct() \
               .zipWithIndex() \
               .map(lambda ((u, (t, c)), d): {"doc_id": d, "title": t, "url": u, "content": c}) \
               .map(lambda x: json.dumps(x)) \
               .saveAsTextFile(load.DOCUMENTS_PATH)

    print 'ok'
    print datetime.datetime.today()

