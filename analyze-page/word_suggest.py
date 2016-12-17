# -*- coding: utf-8 -*-

if __name__ == '__main__':

  from pyspark import SparkConf, SparkContext
  import json

#import load_local as load
  import load
  import parser
  import util

  sc = SparkContext(appName="WordSuggest")

  word_suggest = sc.textFile(load.WORD_LIST_PATH) \
                  .map(util.encode) \
                  .map(lambda x: x["word"].encode("utf-8")) \
                  .map(lambda word: (word, parser.to_hiragana(word))) \
                  .filter(lambda (word, kana): kana is not None) \
                  .map(lambda (word, kana): {"kana": kana, "word": word}) \
                  .map(lambda x: json.dumps(x)) \
                  .saveAsTextFile(load.WORD_SUGGEST_PATH) 

