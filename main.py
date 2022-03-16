from elasticsearch import Elasticsearch

es = Elasticsearch(hosts="http://localhost:9200/")

es.get(index="*", id="1")