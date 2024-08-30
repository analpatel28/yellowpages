import datetime
import json
import os
import re
from multiprocessing import Process
import pymysql

import pandas as pd
import requests
from lxml import html
from uszipcode import SearchEngine,search

count = 1

db_host = 'localhost'
db_user = 'root'
db_password = '2802'

db_name = 'yellowpages'
db_table_name = 'state_data'


connection = pymysql.connect(host=db_host,
                             user=db_user,
                             password=db_password,
                             charset='utf8mb4',)
cursor = connection.cursor()


def create_database():
    try:
        sql = f"CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_general_ci;"
        cursor.execute(sql)
        cursor.execute(f"use {db_name}")
        connection.commit()
    except Exception as e:
        print(e)



def create_table():
    try:

        table_data = f"""CREATE TABLE if not exists {db_table_name}(
              `id` int(11) NOT NULL AUTO_INCREMENT,
              `ADDRESS` varchar(255) UNIQUE,              
              `CFM_ID` varchar(255),              
              `CITY` varchar(255),              
              `COMPANY` varchar(255),              
              `CONTACT_1` varchar(255),              
              `CONTACT_2` varchar(255),              
              `FAX` varchar(255),              
              `INDUSTRY` varchar(255),              
              `PHONE` varchar(255),              
              `STATE` varchar(255),              
              `ZIP` varchar(255),              
              `ACCOUNT_ID` varchar(255),              
              `CATEGORY` varchar(255),              
              `PREFERRED` varchar(255),              
              `EMAIL` varchar(255),              
              `WEBSITE` varchar(255),              
              `URL` varchar(255),              
              `SOURCE` varchar(255),              
              `SCRAPED_TIME` varchar(255),              
               PRIMARY KEY (`id`)) """

        cursor.execute(table_data)

    except Exception as e:
        print(e)


def get_data(link,key,st):
        try:
            link_req = requests.get(link)
            link_res = html.fromstring(link_req.text)
            try:
                company = link_res.xpath('//div[@class="company-main-info"]/h1/text()')[0]
            except:
                company = ''
            try:
                address = link_res.xpath('//span[@itemprop="streetAddress"]/text()')[0]
            except:
                address = ''
            try:
                city = re.findall('"addressLocality": "(.*?)",', link_req.text)[0]
            except:
                city = ''
            zipco = re.findall('"postalCode": "(.*?)",', link_req.text)[0]
            search = SearchEngine()
            zipcode = search.by_zipcode(zipco)
            z = zipcode.to_json()
            r = json.loads(z)
            try:
                state = r['state']
            except:
                pass
            try:
                postalCode = zipco
            except:
                postalCode = ''

            try:
                telephone = link_res.xpath('//dt[contains(text(),"Phone number")]/following-sibling::dd/a//span[2]/text()')[0]
            except:
                telephone = ''
            if not telephone:
                try:
                    telephone = link_res.xpath('//span[contains(text(),"phone")]/following-sibling::div/span/@data-phone-number')[0]
                except:
                    telephone = ''
            try:
                Email = link_res.xpath('//p[contains(text(),"E-mail:")]/text()')[0]
            except:
                Email = ''
            try:
                website = link_res.xpath('//dt[contains(text(),"Linki")]/following-sibling::dd/div/a/text()')[0].replace('\n', '')
            except:
                website = ''
            if not website:
                try:
                    website = link_res.xpath('//span[contains(text(),"launch")]/following-sibling::div/a/text()')[0].replace('\n','')
                except:
                    website = ''
                total = {
                'ADDRESS': address,
                'CFM_ID': '003YP',
                'CITY': city,
                'COMPANY': company,
                'CONTACT_1': '',
                'CONTACT_2': '',
                'FAX': '',
                'INDUSTRY': key,
                'PHONE': telephone,
                'STATE': state,
                'ZIP': postalCode,
                'ACCOUNT_ID': '',
                'CATEGORY': key,
                # 'PREFERRED': var,
                'EMAIL': Email,
                'WEBSITE': website,
                'URL': link,
                'SOURCE': 'https://www.yellowpages.com',
                'SCRAPED_TIME': datetime.datetime.now().strftime("%d/%m/%Y"),
            }

            my_list = [total]
            print(total)
            filename = 'Yellowpages.csv'
            df = pd.DataFrame(my_list)
            if os.path.exists(f'{filename}'):
                df.to_csv(f'{filename}', mode='a', index=False, header=False)
            else:
                df.to_csv(f'{filename}', mode='a', index=False, header=True)

            field_list = []
            value_list = []
            for field in total:
                field_list.append(str(field))
                value_list.append(str(total[field]).replace("'", "’"))
            fields = ','.join(field_list)
            values = "','".join(value_list)

            try:
                insert_db = f"insert into {db_name}.{db_table_name} " + "( " + fields + " ) values ( '" + values + "' )"
                cursor.execute(insert_db)
                print('data successfully inserted!!!')
                connection.commit()

            except Exception as e:
                print(e)
                pass
        except:
            print('URL : ', link)
            pass



def link_data():


        processes = []
        kwords = []
        sts = []
        with open('k2.txt') as c:
            text = c.readlines()
            urls = ([s.strip('\n') for s in text])
            for url in urls:
                kwords.append(url)

        with open('s1.txt') as c:
            text = c.readlines()
            urls = ([s.strip('\n') for s in text])
            for url in urls:
                sts.append(url)
        for st in sts:
            for key in kwords:
                page = 1
                next = True
                print('state: ', st)
                print('Keyword: ', key)
                while next:
                    # url = f'https://www.yellowpages.net/places/doctor/New-York,-Ny,-Usa/{page}.html'.replace(' ', '+')
                    url = f'https://www.yellowpages.net/places/doctors/Alabama/{page}.html'.replace(' ', '+')
                    try:
                        print(url)
                        response = requests.get(url)
                        res = html.fromstring(response.text)
                        links = res.xpath('//h2[@class="card__title mdc-typography--headline6"]/a')
                        if not links:
                            next = False
                            continue
                        for i in links:
                            try:
                                link = i.xpath('./@href')[0]
                            except:
                                link = ''
                            if not link:
                                continue
                            t1 = Process(target=get_data, args=(link,key,st))
                            t1.start()
                            processes.append(t1)
                        for k in processes:
                            k.join()
                        page += 1
                    except:
                       pass











if __name__ == '__main__':
    create_database()
    create_table()
    link_data()
