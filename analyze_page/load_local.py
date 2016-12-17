# coding: UTF-8

from pyspark import SparkConf, SparkContext

WORD_LIST_PATH = "../data/min/word_list/"
WORD_SUGGEST_PATH = "../data/min/word_suggest/"
DOCUMENTS_PATH = "../data/min/documents/"
INVERTED_INDEX_PATH = "../data/min/inverted_index/"
PARSED_PAGE_PATH = "../data/min/parsed_page/"
DATA_PATH = "../data/min/raw/page_data_1.txt"

def load_page(sc):
  return sc.textFile(DATA_PATH)

if __name__ == '__main__':
  sc = SparkContext(appName="Load Test")
  page_rdd = load_page(sc)
  print page_rdd.count()
  for b in page_rdd.collect():
    print b.encode("utf-8")
