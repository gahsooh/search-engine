# coding: utf-8
from bs4 import BeautifulSoup as bs
import MeCab, jctconv
from collections import Counter
import copy, re


def parsePage(page):
    try:
      page_title = page['pageTitle']
      entry_title = page['entryTitle']
      title = entry_title + ' | ' + page_title
      entry_content = page['entryContent']
      url = get_url(page)
      
      soup = bs(entry_content)
      sentences = removeNoise(soup.strings)
      barrel, anchors = getBarrelWithSoup(soup, sentences, title.encode('utf-8'), url)
      links = [(url, a) for a in anchors]
    except:
      return None, None
    
    return barrel, links

def removeNoise(sentences):
    new_sentences = []
    
    for s in sentences:
        if len(s) <= 1 or '\n' in s: continue
        new_sentences.append(s.encode('utf-8'))
            
    return new_sentences

def getBarrelWithSoup(soup, sentences, title, url):
    anchors = []
    elems = {}
    words_in_sentences = []
    content = ""
    inverted_index_list = []
    words_in_title = morphologicalAnalysis(title)
    words_in_sentences += words_in_title
    elems = makeTitleElem(elems, words_in_title)
    for s in sentences:
        tag = []
        attr = []
        anchor = []
        inverted_index = {}
        for p in soup.find(text = s).parents:
            if p.name == 'body': break
            tag.append(p.name)
            
            if 'style' not in p.attrs.keys(): continue
            attr.append(p['style'])
            
            if p.name != 'a' or 'href' not in p.attrs: continue
            anchor_url = p['href']
            anchor.append(anchor_url)
            anchors.append(anchor_url)
            
        words_in_sentence = morphologicalAnalysis(s)
        elems = makeSentenceElem(elems, words_in_sentence, tag, attr, words_in_title)
        words_in_sentences += words_in_sentence
        content = content + ' ' + s.translate(None, ' ')
    
    tf_doc = getTf(words_in_sentences)
    words_with_meta = [(word, (v['header'], v['style'], v['title'], tf_doc[word])) for word, v in elems.items()]
        
    barrel = {
               'title': title, 
               'url': url,
               'content': content,
               'words': words_with_meta,
             }
    
    return barrel, anchors

def makeTitleElem(elems, words):
    for w in list(set(words)):
        elems.update({w: {'header': 0, 'style': 0, 'title': 1}})
    return elems

def makeSentenceElem(elems, words, tag, attr, words_in_title):
    for w in list(set(words)):
        if w in words_in_title: continue
        h = 1 if 'h1' in tag or 'h2' in tag or 'h3' in tag or 'h4' in tag or 'h1' in tag else 0
        s = 1 if len(attr) > 0 else 0
        elems.update({w: {'header': h, 'style': s, 'title': 0}})
    return elems

def getTf(words):
    tf = Counter(words).most_common()
    doc_size = len(words)
    normalized_tf = {}

    if doc_size == 0:
      return {}

    for word, freq in tf:
      normalized_tf[word] = round(freq / float(doc_size), 5)

    return normalized_tf

    


def getBarrel(page):
    content = flattenText(page.text)
    words = morphologicalAnalysis(content)
    unique_words = list(set(words))
    tf_in_page = getTf(words)
    words_with_meta = integrateWordsAttr(
        words, page._id, page._title, tf_in_page)

    barrel = {
        'page_id': page._id,
        'title': page._title,
        'url': page._url,
        'content': content,
        'words': words,
        'unique_words': unique_words,
        'words_with_meta': words_with_meta
    }

    return barrel

def flattenText(text):
    delete = {ord(u'\n'): None}
    return text.translate(delete)

def integrateWordsAttr(words, page_id, title, tf):
    return [(w, (page_id, w == title, tf[w])) for w in words]

# 形態素解析
def morphologicalAnalysis(sentence):
    mecab = MeCab.Tagger()
    morpheme = mecab.parse(sentence.encode('utf-8'))
    morpheme = morpheme.splitlines()
    words = preprocessePartsOfSpeech(morpheme)
    # words = preprocesseJct(words)
    return words

# 品詞処理
def preprocessePartsOfSpeech(morpheme):
    words = []
    for m in morpheme:
        recode = m.split()
        if recode[0] == 'EOS': continue
        if recode[1] == '名詞,サ変接続,*,*,*,*,*': continue
        if recode[1] == '名詞,数,*,*,*,*,*': continue
        pos = recode[1].split(',')
        if pos[0] not in ['名詞', '動詞', '形容詞']: continue
        word = recode[0] if pos[0] == '名詞' else pos[6]
        if inHalfKatakana(word): continue
        if isHiragana(word) and len(unicode(word, "utf-8")) <= 2: continue
        if isHiragana(word) and len(unicode(word, "utf-8")) >= 7: continue
        if isAlphabet(word) and len(unicode(word, "utf-8")) == 1: continue
        words.append(word)
    return words

# 漢字・ひらがな・カタカナ処理
def preprocesseJct(words):
    new_words = copy.deepcopy(words)
    mecab = MeCab.Tagger()
    for w in words:
        m = mecab.parse(w)
        recode = m.split()
        if recode[0] == 'EOS': continue
        recode = recode[1].split(',')
        if len(recode) < 8: continue
        new_words.append(recode[7])
        unicode_recode = jctconv.kata2hira(unicode(recode[7],'utf-8'))
        new_words.append(unicode_recode.encode('utf-8'))
        
    new_words = list(set(new_words))
    return new_words

def isHiragana(word):
    word = word.decode("utf-8")
    a =   [ch for ch in word if u"ぁ" <= ch <= u"ん" or ch == u"ー"]
    if len(word) == len(a):
        return True
    return False

def isKatakana(word):
    word = word.decode("utf-8")
    a =   [ch for ch in word if u"ァ" <= ch <= u"ン" or ch == u"ー"]
    if len(word) == len(a):
        return True
    return False

def toHiragana(word):
    if isKatakana(word) or isHiragana(word):
      return jctconv.kata2hira(unicode(word,'utf-8')).encode('utf-8')

    mecab = MeCab.Tagger()
    m = mecab.parse(word)
    recode = m.split()
    if recode[0] == 'EOS':
	return None
    recode = recode[1].split(',')
    if len(recode) < 8:
	return None
    unicode_recode = jctconv.kata2hira(unicode(recode[7],'utf-8'))
    return unicode_recode.encode('utf-8')

def inHalfKatakana(word):
    word = word.decode("utf-8")
    for ch in word:
      if u"ｦ" <= ch <= u"ﾝ": return True
    return False 

def isAlphabet(word):
    word = word.decode("utf-8")
    a =   [ch for ch in word if u"A" <= ch <= u"z" or ch == u"-"]
    if len(word) == len(a):
        return True
    return False
