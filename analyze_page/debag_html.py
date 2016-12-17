# coding: utf-8
from bs4 import BeautifulSoup as bs
from pyspark import SparkConf, SparkContext
import util
import load_local as load

def get_html(blog):
    try:
      entry_content = blog['entryContent']
      soup = bs(entry_content)
      beautiful_html = soup.prettify()
    except:
      return None
    
    return beautiful_html


### test code ###
if __name__ == '__main__':
	sc = SparkContext(appName="Check HTML")

	print 'start'
	blog_rdd = sc.textFile(load.DATA_PATH).map(util.encode)
	blog_rdd.map(get_html) \
			.saveAsTextFile('./debag/html')
	print 'ok'