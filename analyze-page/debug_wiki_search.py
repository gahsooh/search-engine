# coding: utf-8

import copy, datetime, json, sys
from pyspark import SparkConf, SparkContext
from operator import add

import load_local
import pagerank
import parser
import util

WORD_LIST_SAVE_PATH = 'spark-warehouse/word_list'
DOCUMENTS_SAVE_PATH = 'spark-warehouse/documents'
INVERTED_INDEX_SAVE_PATH = 'spark-warehouse/inverted_index'


### test code ###
if __name__ == '__main__':
	sc = SparkContext("local", "Main Test")
	worker_size = 5

	print 'start parse'
	page_rdd = load_local.pagesToDataframe(sc).rdd
	barrel_rdd = page_rdd.map(parser.getBarrel)

	print 'start make wordID list'
	word_to_wordID_rdd = (barrel_rdd
							.flatMap(lambda x: x['words'])
							.distinct()
							.zipWithIndex()
							.cache())

	(word_to_wordID_rdd.map(lambda (w, w_id): {
									"word": w, 
									"word_id": w_id
									})
									.map(lambda x: json.dumps(x))
									.saveAsTextFile(WORD_LIST_SAVE_PATH))

	word_to_wordID = word_to_wordID_rdd.collectAsMap()

	print 'start store document list'
	documents_rdd = barrel_rdd.map(lambda x: 
		(x['page_id'], x['title'], x['url'], x['content']))

	(documents_rdd.distinct()
					.map(lambda (p, t, u, c): {
								"page_id": p, 
								"title": t, 
								"url": u, 
								"content": c
								})
								.map(lambda x: json.dumps(x))
								.saveAsTextFile(DOCUMENTS_SAVE_PATH))

	print 'start get pagerank'
	pageID_rdd = barrel_rdd.map(lambda x: x['page_id'])
	title_to_pageID = (barrel_rdd.map(lambda x: (x['title'], x['page_id']))
						.distinct()
						.collectAsMap())
	links_rdd = (load_local.pagelinksToDataframe(sc).rdd
					.map(lambda x: (x.pl_from, title_to_pageID.get(x.pl_title, -1))))
	ranks_rdd = pagerank.pagerank(pageID_rdd, links_rdd, 5)

	print 'start get tf'
	tf_rdd = (barrel_rdd
				.flatMap(lambda x: x['words_with_meta'])
				.map(lambda (word, (page_id, is_title, tf)):
					(word_to_wordID[word], (page_id, tf)))
				.cache())

	print 'start get df'
	df_rdd = (barrel_rdd
				.flatMap(lambda x: x['unique_words'])
				.map(lambda word: (word_to_wordID[word], 1))
				.reduceByKey(add)
				.cache())

	print 'start get tfidf'
	tfidf_rdd = (tf_rdd
					.join(df_rdd)
					.map(lambda (word_id, ((page_id, tf), df)): 
						((word_id, page_id), round(tf / float(df), 5)))
					.cache())

	print 'start inverted index'
	inverted_index_rdd = (barrel_rdd
							.flatMap(lambda x: x['words_with_meta'])
							.map(lambda (word, (page_id, is_title, tf)):
								((word_to_wordID[word], page_id), is_title))
							.join(tfidf_rdd)
							.map(lambda ((word_id, page_id), (is_title, tfidf)): 
								(page_id, (word_id, tfidf, is_title)))
							.join(ranks_rdd)
							.map(lambda (page_id, ((word_id, tfidf, is_title), rank)): 
								(word_id, (page_id, tfidf, is_title, rank))))

	(inverted_index_rdd.map(lambda (w_id, (p, tfidf, is_title, r)): 
							(w_id, {
								"page_id": p, 
								"tfidf": tfidf, 
								"is_title": is_title, 
								"pagerank": r
							}))
							.groupByKey()
							.map(lambda (w_id, index): 
								{"word_id": w_id, "index": list(index)})
							.map(lambda x: json.dumps(x))
							.saveAsTextFile(INVERTED_INDEX_SAVE_PATH))


