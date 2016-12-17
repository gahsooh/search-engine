# coding: utf-8

import json

import load as ld

### from_entryID_to_docID_map ###
def get_entryID_to_docID_map(page_rdd):
    entryIDs = page_rdd.map(lambda x: x['entryId']).collect()
    docIDs = xrange(1, len(entryIDs)+1)
    return dict(zip(entryIDs, docIDs))

### from_url_to_docID_map ###
def get_url_to_docID_map(page_rdd):
    return page_rdd.map(url_to_docID).collectAsMap()

def url_to_docID(page): 
    url = to_url(page['amebaId'], page['entryId'])
    docID = entryID_to_docID_map[page['entryId']]
    return (url, docID)

def to_url(amebaID, entryID):
    return 'http://ameblo.jp/'+ str(amebaID) +'/entry-'+ str(entryID) +'.html'

### common ###
def encode_page(page):
    page = page.encode('utf-8') if type(page) == unicode else page
    return json.loads(page)
    