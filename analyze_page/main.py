# coding: utf-8

import json, sys, copy, datetime
from pyspark import SparkConf, SparkContext
from operator import add

import load
import pagerank
import parser
import util


### test code ###
if __name__ == '__main__':
    sc = SparkContext("local", "Main Test")

    print datetime.datetime.today()
    worker_size = 5
    
    print 'start load'
    blog_rdd = (load.load_ameblo(sc)
                .map(util.encode))
    
    print 'start make map'
    url_to_docID_rdd = (blog_rdd
                        .map(parser.get_url)
                        .distinct()
                        .zipWithIndex()
                        .cache())
    url_to_docID = url_to_docID_rdd.collectAsMap()
    
    print 'start parse'
    parsed_blog_rdd = blog_rdd.map(lambda x: parser.parse_blog(x, url_to_docID))
    barrels_rdd = parsed_blog_rdd.keys()
    links_rdd = parsed_blog_rdd.values()
    
    print 'start make wordID list'
    word_to_wordID_rdd = (barrels_rdd
                          .flatMap(lambda x: x['words'])
                          .distinct()
                          .zipWithIndex()
                          .cache())
    (word_to_wordID_rdd.map(lambda (w, w_id): {"word": w, "word_id": w_id})
                                            .map(lambda x: json.dumps(x))
                                            .saveAsTextFile('data/word_list'))

    word_to_wordID = word_to_wordID_rdd.collectAsMap()
#     wordID_to_word = word_to_wordID_rdd.map(lambda (x, y): (y, x)).collectAsMap()
    
    print 'start store document list'
    documents_rdd = barrels_rdd.map(lambda x: (x['docID'], x['title'], x['url'], x['content']))
    (documents_rdd.distinct()
                                  .map(lambda (d, t, u, c): {"doc_id": d, "title": t, "url": u, "content": c})
                                  .map(lambda x: json.dumps(x))
                                  .saveAsTextFile('data/documents'))
        
    print 'start links'
    links_rdd = (links_rdd
                 .flatMap(lambda x: x)
                 .filter(lambda (url1, url2): url2 != -1))
    
    print 'start get pagerank'
    docID_rdd = url_to_docID_rdd.values()
    ranks_rdd = pagerank.pagerank(docID_rdd, links_rdd, 5)
    
    print 'start get tf'
    tf_rdd = (barrels_rdd
              .map(lambda x: x['tf'])
              .flatMap(lambda (docID, tf_list): ((docID, tf) for tf in tf_list))
              .map(lambda (docID, (word, score)): (word_to_wordID[word], (docID, score)))
              .cache())
 
    print 'start get df'
    df_rdd = (barrels_rdd
              .flatMap(lambda x: x['unique_words'])
              .map(lambda word: (word_to_wordID[word], 1))
              .reduceByKey(add)
              .cache())
    
    print 'start get tfidf'
    tfidf_rdd = (tf_rdd
                 .join(df_rdd)
                 .map(lambda (wordID, ((docID, tf_score), df_score)): ((wordID, docID), round(tf_score * df_score, 5)))
                 .cache())
    
    print 'start inverted index'
    inverted_index_rdd = (barrels_rdd
                          .map(lambda x: x['elems'])
                          .flatMap(lambda (docID, elem_list): ((docID, elem) for elem in elem_list))
                          .map(lambda (docID, (word, elem)): ((word_to_wordID[word], docID), elem))
                          .join(tfidf_rdd)
                          .map(lambda ((wordID, docID), ((header, style, title), tfidf)): (docID, (wordID, tfidf, header, style, title)))
                          .join(ranks_rdd)
                          .map(lambda (docID, ((wordID, tfidf, header, style, title), rank)): (wordID, (docID, tfidf, header, style, title, rank))))
    
    (inverted_index_rdd.map(lambda (w_id, (d, tfidf, h, s, title, r)): (w_id, {"doc_id": d, "tfidf": tfidf, "header": h, "style": s, "title": title, "pagerank": r}))
                                 .groupByKey()
                                 .map(lambda (wordID, elem): {"word_id": wordID, "index": list(elem)})
                                 .map(lambda x: json.dumps(x))
                                 .saveAsTextFile('data/inverted_index'))
    
###################################################################
        
    inverted_index_rdd.collect()

    print 'ok'
    print datetime.datetime.today()

