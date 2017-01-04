# coding: UTF-8

from pyspark import SparkConf, SparkContext

WORD_LIST_PATH = "s3n://"
WORD_SUGGEST_PATH = "s3n://"
DOCUMENTS_PATH = "s3n://"
INVERTED_INDEX_PATH = "s3n://"
PARSED_PAGE_PATH = "s3n://"
DATA_PATH = "s3n://search-engine-storage/min/0000.xml"

def loadPage(sc):
  return sc.textFile(DATA_PATH)

if __name__ == '__main__':
  sc = SparkContext(appName="Load Test")
  page_rdd = load_page(sc) 
  for b in page_rdd.collect():
    print b.encode("utf-8")
