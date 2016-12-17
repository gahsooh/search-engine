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
	sc = SparkContext(appName="Check Documents") 

	with open("debag/documents", "w+") as file:
		file.write("### ### ### ### ### ### ### ### ### debag ### ### ### ### ### ### ### ### ###\n\n")

	print 'start load'
	documents_rdd = sc.textFile(load.DOCUMENTS_PATH) \
					  .map(util.encode)

	for d in documents_rdd.collect():
		with open("debag/documents", "a") as file:
			for k, v in d.items():
				file.write(str(encode(k)) + ': ' + str(encode(v)) + '\n')
			file.write('\n#########################################\n\n')

	print 'ok.'
	print 'check ./debag/documents'
