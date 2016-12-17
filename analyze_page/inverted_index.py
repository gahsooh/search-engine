# coding: utf-8

import json, sys, copy, datetime
from pyspark import SparkConf, SparkContext
from operator import add
from more_itertools import chunked
import math

import load
#import load_local as load
import pagerank
import parser
import util


### test code ###
if __name__ == '__main__':
    sc = SparkContext(appName="Inverted Index")
    DIVIDE_INDEX = 20

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
    links_rdd = parsed_page_rdd.values().flatMap(lambda x: x)
    
    word_word_id = sc.textFile(load.WORD_LIST_PATH).map(util.encode).map(lambda x: (x['word'], x['word_id']))
    
    print 'start links'
    
    print 'start get pagerank'

    ranks_rdd = pagerank.pagerank(doc_id_rdd, links_rdd, 5)
    
    # Create tf idf and inverted index
    MINIMUM_VALID_DF = 10
    words_with_meta = docid_words.join(ranks_rdd) \
                                 .flatMap(lambda (doc_id, (words, rank)): [(word, (doc_id, meta, rank)) for (word, meta) in words]) \
                                 .join(word_word_id) \
                                 .map(lambda (word, ((doc_id, meta, rank), word_id)): ((word_id, word), (doc_id, meta, rank))) \
                                 .groupByKey() \
                                 .map(lambda (word_with_id, docs): (word_with_id, docs, len(docs))) \
                                 .filter(lambda (word_with_id, docs, df): df > MINIMUM_VALID_DF)
    words_with_meta.cache()

    words_with_meta.map(lambda ((word_id, word), docs, df): (word_id, [(doc_id, h, s, t, round(tf * df, 5), rank) for (doc_id, (h, s, t, tf), rank) in docs])) \
                   .map(lambda (word_id, docs): (word_id, [{"doc_id": doc_id, "tfidf": tfidf, "header": h, "style": s, "title": t, "pagerank": r} for (doc_id, h, s, t, tfidf, r) in docs])) \
                   .flatMap(lambda (word_id, docs): [(word_id, i, sub_docs) for i, sub_docs in enumerate(list(chunked(docs, int(math.ceil(len(docs)/float(DIVIDE_INDEX))))))]) \
                   .map(lambda (word_id, i, docs): {"word_id": (str(word_id) + "-" + str(i)), "index": docs}) \
                   .map(lambda x: json.dumps(x)) \
                   .saveAsTextFile(load.INVERTED_INDEX_PATH)

    words_with_meta.map(lambda ((word_id, word), docs, df): (word, parser.to_hiragana(word.encode("utf-8")), df)) \
                   .filter(lambda (word, kana, df): kana is not None) \
                   .filter(lambda (word, kana, df): df > MINIMUM_VALID_DF) \
                   .map(lambda (word, kana, df): {"word": word, "kana": kana, "df": df}) \
                   .map(lambda x: json.dumps(x)) \
                   .saveAsTextFile(load.WORD_SUGGEST_PATH)

    print 'ok'
    print datetime.datetime.today()

