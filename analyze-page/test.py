# coding: UTF-8

import json
from pymongo import MongoClient

### test func ###
def create_wordID_index(wordID, index_server_id):
    
    wordID_index = {}
    
    wordID_index.update({'wordID': wordID})
    
    wordID_attribution = {
                "docID": 5131104,
                "location": 11,
                "title": 1,
                "size": 18,
                "header": 0,
                "tfidf": 11
    }
    index = []
    index.append(wordID_attribution)
    index.append(wordID_attribution)
    index.append(wordID_attribution)
    wordID_index.update({'index': index})
    
    wordID_index.update({'index_server_id': index_server_id})
    
    return wordID_index

def show_log(log):
    result = log.first()
    jsonData = json.loads(result)

    print jsonData.keys()
    for key in jsonData.keys():
        print key
        print jsonData[key], '\n'
        
        
def show_mongo():
    connection = MongoClient('54.199.161.16', 27017)
    db = connection.test
    mongoDB_collection = db.documents
    
    for data in mongoDB_collection.find():
        print data
        
def show(x): print(x)
    
def load_blog(sc): return sc.textFile('./tmp/page_data_small.txt')