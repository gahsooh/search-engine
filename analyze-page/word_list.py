# coding: utf-8

import json, sys, copy, datetime
from pyspark import SparkConf, SparkContext
from operator import add

#import load_local as load
import load
import pagerank
import parser
import util


### test code ###
if __name__ == '__main__':
  sc = SparkContext(appName="WordList")

  print datetime.datetime.today(), 'start load'

  barrels_rdd = sc.textFile(load.PARSED_PAGE_PATH) .map(util.encode).keys()
  
  word_to_wordID_rdd = barrels_rdd.flatMap(lambda x: [word for word, meta in x['words']]) \
                        .distinct() \
                        .zipWithIndex() \
                        .map(lambda (w, w_id): {"word": w, "word_id": w_id}) \
                        .map(lambda x: json.dumps(x)) \
                        .saveAsTextFile(load.WORD_LIST_PATH)
  print 'ok'
  print 'end make word list', datetime.datetime.today()

