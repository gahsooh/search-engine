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


### test code ###
if __name__ == '__main__':
	sc = SparkContext("local", "Main Test")
	worker_size = 5

	print 'start parse'
	page_rdd = load_local.xml_to_dataframe(sc).rdd
	barrel_rdd = page_rdd.map(parser.get_barrel_)

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

	print 'start get tf'
	tf_rdd = (barrel_rdd
				.map(lambda x: (x['page_id'], x['words_with_meta']))
				.map(lambda (page_id, (word, (is_title, tf))):
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
					.map(lambda (wordID, ((docID, tf_score), df_score)): 
						((wordID, docID), round(tf_score * df_score, 5)))
					.cache())


