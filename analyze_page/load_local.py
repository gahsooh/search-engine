# coding: UTF-8

WORD_LIST_PATH = "./data/word_list/"
WORD_SUGGEST_PATH = "./data/word_suggest/"
DOCUMENTS_PATH = "./data/documents/"
INVERTED_INDEX_PATH = "./data/inverted_index/"
PARSED_PAGE_PATH = "./data/parsed_blog/"
DATA_PATH = "data/blog_data_100.txt"


def load_data(sc):
  return sc.textFile(DATA_PATH)

if __name__ == '__main__':
  from pyspark import SparkConf, SparkContext
  sc = SparkContext(appName="Load Test")
  blog_rdd = load_data(sc) 
  for b in blog_rdd.collect():
    print b.encode("utf-8")
