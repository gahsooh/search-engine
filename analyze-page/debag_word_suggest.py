# coding: utf-8

import json
from pyspark import SparkConf, SparkContext
import load_local as load
#import load
import util

def encode(x):
	return x.encode('utf-8') if type(x) == unicode else x

### test code ###
if __name__ == '__main__':
	sc = SparkContext(appName="Check Word Suggest")  

	with open("debag/word_suggest", "w+") as file:
		file.write("### ### ### ### ### ### ### ### ### debag ### ### ### ### ### ### ### ### ###\n\n")

	print 'start load'
	word_suggest_rdd = sc.textFile(load.WORD_SUGGEST_PATH) \
					  .map(util.encode)

	for w in word_suggest_rdd.collect():
		with open("debag/word_suggest", "a") as file:
			for v in w.values():
				file.write(str(encode(v))+'\t')
			file.write('\n')

	print 'ok'