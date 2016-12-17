# coding: utf-8

import json, sys, copy, datetime
from pyspark import SparkConf, SparkContext
from operator import add
import os

import load_local as load
#import load

import pagerank
import parser
import util


### test code ###
if __name__ == '__main__':
    sc = SparkContext(appName="Preprocess")

    print datetime.datetime.today()
    
    print 'start load'
    blog_rdd = sc.textFile(load.DATA_PATH).map(util.encode)
    
    print 'start make map'
    
    print 'start parse'
    parsed_blog_rdd = blog_rdd.map(lambda blog: parser.parse_blog(blog))
    parsed_blog_rdd.cache()
    parsed_blog_rdd.map(lambda x: json.dumps(x)) \
                   .saveAsTextFile(load.PARSED_PAGE_PATH)

    barrels_rdd = parsed_blog_rdd.keys()
    barrels_rdd.cache()

    print 'start store document list'
    documents = barrels_rdd.map(lambda x: (x['url'], (x['title'], x['content']))) \
                           .distinct() \
                           .zipWithIndex() \
                           .map(lambda ((u, (t, c)), d): {"doc_id": d, "title": t, "url": u, "content": c}) \
                           .map(lambda x: json.dumps(x)) \
                           .saveAsTextFile(load.DOCUMENTS_PATH)
        
    print 'start make wordID list'
    word_to_wordID_rdd = barrels_rdd.flatMap(lambda x: x['words']) \
                                    .distinct() \
                                    .zipWithIndex() \
                                    .cache()
    word_to_wordID_rdd.map(lambda (w, w_id): {"word": w, "word_id": w_id}) \
                      .map(lambda x: json.dumps(x)) \
                      .saveAsTextFile(load.WORD_LIST_PATH)

    print 'ok'
    print datetime.datetime.today()

