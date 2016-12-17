from operator import add

def calculateContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)
        
def scoring(score, init_score):
    new_score = init_score * 0.15
    if score is not None:
        new_score += score * 0.85
    return new_score

def pagerank(docID_rdd, links, n):
    init_ranks = docID_rdd.map(lambda docID: (docID, 1.0))
    init_ranks.cache()
    ranks = init_ranks
    links = links.groupByKey().mapValues(list)
    links.cache()

    if links.count() == 0:
      return ranks

    for i in range(n):
        cont = links.join(ranks) \
                    .flatMap(lambda url_urls_rank : calculateContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        ranks = cont.reduceByKey(add) \
                    .rightOuterJoin(init_ranks) \
                    .map(lambda (docID, (score , init_score)): (docID, scoring(score, init_score)))
    return ranks
