from elasticsearch import Elasticsearch

es = Elasticsearch(hosts="http://localhost:9200/")
find_all = es.search(body={
    "query": {
        "match_all": {}
    }
})

find_by_id = es.search(body={
    "query": {
        "terms": {
            "_id": ["vWAOm38B0TAjmiDsT0EZ"]
        }
    }
})

find_by_sentence = es.search(body={
    "from": 0,
    "size": 10,
    "query": {
        "match": {
            "content": "Covid-19"
        }
    }
})

print(find_by_sentence)
