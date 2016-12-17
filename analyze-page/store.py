# coding: UTF-8

import json, os, boto3
from pymongo import MongoClient as mc

import load as ld
import test

### from Backend Server to S3 (inverted_index) ###  
def make_output_file(index_filename, mode, json_wordID_index):
    with open(index_filename, mode) as f:
        f.write(json_wordID_index)
        f.write('\n')

def store_index(mode, wordID_index):
    output_dir = './tmp/'
    index_filename = 'page_data_output.txt'
    output_path = output_dir + index_filename

    json_wordID_index = json.dumps(wordID_index)
    make_output_file(output_path, mode, json_wordID_index)

    s3_client = boto3.client('s3')
    s3_client.upload_file(output_path, 'bucket_name', index_filename)

        
### from Backend Server to mongoDB (docID-document) ### 
def store_documents(log):
    store_document(json.loads(log.first()))

def store_document(first_log):
    docID = 3
    page = {k: encode_utf8(v) for k, v in first_log.items()}
    recode = create_recode(docID, page)
    
    mongoDB_collection = init_mongoDB()
    mongoDB_collection.insert_one(recode)

def create_recode(docID, page):
    recode = {}
    recode.update({'doc_id': docID})
    recode.update({'title': str(page['pageTitle'])})
    recode.update({'url': ''})
    recode.update({'content': str(page['entryContent'])})
    return recode

def init_mongoDB():
    connection = mc('54.199.161.16', 27017)
    db = connection.test
    return db.documents

def encode_utf8(value):
    return value.encode('utf-8') if type(value) == unicode else value