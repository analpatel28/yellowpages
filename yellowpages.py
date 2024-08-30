import json
import re

import requests
from lxml import html
from uszipcode import SearchEngine, search

page = 1
next = True
while next:
    url = f'https://www.yellowpages.net/places/doctor/New-York,-Ny,-Usa/{page}.html'
    print(url)
    req = requests.get(url)
    res = html.fromstring(req.text)
    links = res.xpath('//h2[@class="card__title mdc-typography--headline6"]/a')
    if not links:
        next = False
        page = 1
        break

    for i in links:
        link = i.xpath('./@href')[0]
        link_req = requests.get(link)
        link_res = html.fromstring(link_req.text)
        item ={}
        item['Url'] = link
        try:
            item['company'] = link_res.xpath('//div[@class="company-main-info"]/h1/text()')[0]
        except:
            item['company'] = ''
        try:
            item['address'] = link_res.xpath('//span[@itemprop="streetAddress"]/text()')[0]
        except:
            item['address'] = ''
        try:
            item['city'] = re.findall('"addressLocality": "(.*?)",',link_req.text)[0]
        except:
            item['city'] = ''
        zipco = re.findall('"postalCode": "(.*?)",',link_req.text)[0]
        search = SearchEngine()
        zipcode = search.by_zipcode(zipco)
        z = zipcode.to_json()
        r = json.loads(z)
        try:
            item['state'] = r['state']
        except:
            pass
        try:
            item['postalCode'] = zipco
        except:
            item['postalCode'] = ''

        try:
            item['telephone'] = link_res.xpath('//dt[contains(text(),"Phone number")]/following-sibling::dd/a//span[2]/text()')[0]
        except:
            item['telephone'] = ''
        if not item['telephone']:
            try:
                item['telephone'] = link_res.xpath('//span[contains(text(),"phone")]/following-sibling::div/span/@data-phone-number')[0]
            except:
                item['telephone'] = ''
        try:
            item['Email'] = link_res.xpath('//p[contains(text(),"E-mail:")]/text()')[0]
        except:
            item['Email'] = ''
        try:
            item['website'] = link_res.xpath('//dt[contains(text(),"Linki")]/following-sibling::dd/div/a/text()')[0].replace('\n','')
        except:
            item['website'] = ''
        if not item['website']:
            try:
                item['website'] = link_res.xpath('//span[contains(text(),"launch")]/following-sibling::div/a/text()')[0].replace('\n','')
            except:
                item['website'] = ''
        print(item)
    page += 1
