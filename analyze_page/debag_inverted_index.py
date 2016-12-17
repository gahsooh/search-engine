# coding: utf-8

import json, sys, copy, datetime
from pyspark import SparkConf, SparkContext
from operator import add

#import load
import load_local as load
import pagerank
import parser
import util


### test code ###
if __name__ == '__main__':
    sc = SparkContext(appName="Debag Inverted Index")

    print datetime.datetime.today()

    url_docid = sc.textFile(load.DOCUMENTS_PATH).map(util.encode).map(lambda doc: (doc['url'], doc['doc_id']))
    url_docid.cache()
    doc_id_rdd = url_docid.map(lambda (url, doc_id): doc_id)

    parsed_page_rdd = sc.textFile(load.PARSED_PAGE_PATH).map(util.encode)
    parsed_page_rdd.cache()
    docid_words = parsed_page_rdd.keys() \
                                  .map(lambda x: (x['url'], x['words'])) \
                                  .join(url_docid) \
                                  .map(lambda (url, (words, doc_id)): (doc_id, words))
    docid_words.cache()
    links_rdd = parsed_page_rdd.values()
    
    word_word_id = sc.textFile(load.WORD_LIST_PATH).map(util.encode).map(lambda x: (x['word'], x['word_id']))
    
    print 'start links'
    links_rdd = links_rdd.flatMap(lambda x: x)
    
    print 'start get pagerank'

    ranks_rdd = pagerank.pagerank(doc_id_rdd, links_rdd, 5)
    
    # Create tf idf and inverted index
    words_with_meta = docid_words.join(ranks_rdd) \
                                 .flatMap(lambda (doc_id, (words, rank)): [(word, (doc_id, meta, rank)) for (word, meta) in words]) \
                                 .join(word_word_id) \
                                 .map(lambda (word, ((doc_id, meta, rank), word_id)): (word_id, (doc_id, meta, rank))) \
                                 .groupByKey() \
                                 .map(lambda (word_id, docs): (word_id, docs, len(docs))) \
                                 .map(lambda (word_id, docs, df): (word_id, [(doc_id, h, s, t, round(tf * df, 5), rank) for (doc_id, (h, s, t, tf), rank) in docs])) \
                                 .map(lambda (word_id, docs): (word_id, [{"doc_id": doc_id, "tfidf": tfidf, "header": h, "style": s, "title": t, "pagerank": r} for (doc_id, h, s, t, tfidf, r) in docs])) \
                                 .map(lambda (word_id, docs): {"word_id": word_id, "index": docs}) \
                                 .map(lambda x: json.dumps(x)) \
                                 .repartition(1) \
                                 .saveAsTextFile('./debag/inverted_index')


    print 'ok'
    print datetime.datetime.today()

