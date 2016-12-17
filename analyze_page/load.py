# coding: UTF-8

WORD_LIST_PATH = "s3n://"
WORD_SUGGEST_PATH = "s3n://"
DOCUMENTS_PATH = "s3n://"
INVERTED_INDEX_PATH = "s3n://"
PARSED_PAGE_PATH = "s3n://"
DATA_PATH = "s3n://"

def load_data(sc):
  return sc.textFile(DATA_PATH)

if __name__ == '__main__':
  from pyspark import SparkConf, SparkContext
  sc = SparkContext(appName="Load Test")
  blog_rdd = load_data(sc) 
  for b in blog_rdd.collect():
    print b.encode("utf-8")