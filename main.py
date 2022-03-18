import requests
import time
import os
from pandas import read_csv
from bs4 import BeautifulSoup as bs
from elasticsearch import Elasticsearch

es = Elasticsearch(hosts="http://localhost:9200/")

def save_data(url):
    post = url
    page = requests.get(post).content
    page = bs(page, 'lxml')
    try:
        title = page.find(class_="content-head__title").string
        subtitle = page.find(class_="content-head__subtitle").string
        last_update = page.find(itemprop="datePublished").attrs


        full_text = []
        description = page.findAll(class_="content-text__container")
        for desc in description:
            if "LEIA TAMBÉM:" not in desc.text:
                full_text.append(desc.text.rstrip())
        full_text = [empty_string for empty_string in full_text if empty_string]
        full_text = '\n'.join(full_text)

        doc = {
            'date': last_update['datetime'],
            'title': title,
            'subtitle': subtitle,
            'content': full_text,
            'url': url
        }

        response = es.index(index='g1', doc_type='docs', body=doc)
        time.sleep(0.5)

        return 'success', response



    except Exception as e:
        print(e)
        return 'fail', None

if __name__ == '__main__':
    start_time = time.time()

    filter_by_uf = False
    year = '2022'
    month = '03'
    date = year + '/' + month
    sitemap = 'https://g1.globo.com/sitemap/g1/sitemap.xml'

    if filter_by_uf:
        uf = 'SC'
        df = read_csv('states.csv')
        state = df.loc[df['uf'] == uf]['state'].values[0]

    page = requests.get(sitemap)
    page = bs(page.content, 'html.parser')

    urls = []
    for element in page.findAll('loc'):
        if date in element.text:
            page_lv2 = requests.get(element.text)
            page_lv2 = bs(page_lv2.content, 'html.parser')

            for element_lv2 in page_lv2.findAll('loc'):
                if filter_by_uf:
                    if state in element_lv2.text:
                        urls.append(element_lv2.text)
                else:
                    urls.append(element_lv2.text)






    test = urls[0]

    #save_data(test)
    count = 0
    for url in urls:
        status, response = save_data(url)

        print(status)
        if status == 'success':
            count += 1
            print(response)

    print(f"\nTempo de processamento de {time.time() - start_time} segundos.\n"
          f"Foram inseridas {count} notícias no banco de dados Elasticsearch.")