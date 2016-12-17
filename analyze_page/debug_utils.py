# using: utf-8

from prettyprint import pp as pprint

def show_rdd(rdd):
	for value in rdd.collect():
		pprint(value)

def show_rdd_with_encode(rdd):
	for value in rdd.collect():
		print value.encode("utf-8")