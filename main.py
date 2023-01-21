import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import asyncio
import sql
import logging

logging.basicConfig(level="INFO")
logger = logging.getLogger()
log = logging.FileHandler('log.txt','a')
logger.addHandler(log)

base_url = 'https://eg.hatla2ee.com'
new_cars = []
used_cars = []
new_cars_links = []
used_cars_links = []
existing_columns = []

def get_links_new(page=1):
    logging.info('Getting new links')
    # logging.info(f'Extracting details from page {page}')
    url =f'https://eg.hatla2ee.com/en/new-car/page/{page}'
    r = requests.get(url)
    logging.info(r.status_code)
    soup = BeautifulSoup(r.text,'lxml')
    links =  soup.find_all('a',class_='nCarListData_title')
    for link in links:
        # new_cars_links.append(base_url + link.attrs['href'])   
        full_link = base_url + link.attrs['href']
        logging.info(full_link)
        sql.insert_link('new_cars_links',link= full_link)
    next_page = soup.find_all('a',class_='paginate')[-1].attrs['href'].split("/")[-1]
    if int(next_page)>int(page):    
        get_links_new(next_page)    
    else:
        logging.info('No more pages')
        logging.info(f'Extracted {len(new_cars_links)} links')


def get_links_used(page=1):
    logging.info(f'Extracting details from page {page}')
    url =f'https://eg.hatla2ee.com/en/car/page/{page}'
    r = requests.get(url)
    soup = BeautifulSoup(r.text,'lxml')
    cars = soup.find_all('div', class_='newCarListUnit_header')
    for car in cars:
        link = car.find('a')
        full_link = base_url + link.attrs['href']
        logging.info(full_link)
        sql.insert_link('used_cars_links',link=full_link)
        # with open('usedcars.txt','a') as f:
            # f.write(base_url +link.attrs['href']+'\n')   
    next_page = soup.find_all('a',class_='paginate')[-1].attrs['href'].split("/")[-1]
    if int(next_page)>int(page):    
        get_links_used(next_page)    
    else:
        logging.info('No more pages')


def get_car_detail(url:str)->BeautifulSoup:
    r = requests.get(url)
    logging.info(url)
    return BeautifulSoup(r.text,'lxml'),url

def extract_car_details_new(soup:BeautifulSoup)->None:
    if 'used' in soup.find('h1',class_='mainTitle').get_text().lower():
        logging.info('used car')
        return
    tables = soup.find('div',class_='newCarPricesWrap').find_all('tbody')
    car_name = soup.find('h2',class_='brandCarTitle').get_text().strip()
    logging.info(car_name)
    for table in tables:
        models = table.find_all('tr')
        for model in models:
            details = model.find_all('td')
            if details:
                car = {
                    'id': details[0].find('a').attrs['id'],
                    'name': details[0].get_text().strip(),
                    'price': int(details[1].get_text().strip().replace(' EGP','').replace(',','')) if not details[1].find('del') else int(details[1].find_all('strong')[-1].get_text().strip().replace(' EGP','').replace(',','')),
                    'minimum_deoposit': int(details[2].get_text().strip().replace(' EGP','').replace(',','')),
                    'minimum_installment': int(details[3].get_text().strip().replace(' EGP','').replace(',','')),
                    'CC': details[4].get_text().strip(),
                    'link': details[0].find('a').attrs['href'],
                    'make': details[0].find('a').attrs['href'].split('/')[-3].title(),
                    'model': details[0].find('a').attrs['href'].split('/')[-2].title(),
                }
                new_cars.append(car)
                sql.insert_pgsql_table(car)


def extract_car_details_used(url:str)->None:
    r = requests.get(url)
    soup = BeautifulSoup(r.text,'lxml')
    text = str(soup.find('div',class_='hidden-desktop UnitDescWhatsapp'))
    car = {
    'id': url.split('/')[-1],
    }
    try:
        car['price']= int(soup.find('span',class_='usedUnitCarPrice').get_text().strip().strip(' EGP').replace(',',''))
    except:
        return
    if soup.find('strong',title="Installment"):
        try:
            car['installment'] = int(soup.find('strong',title="Installment").get_text().strip().strip(' EGP').replace(',',''))
        except:
            pass
        try:
            car['deposit'] = int(soup.find('strong',title="Deposit").get_text().strip(' EGP').replace(',',''))
        except:
            pass    
    if re.findall(r'\+\d+',text):
        car['phone_number']= re.findall(r'\+\d+',text)[0]
    details = soup.find_all('div',class_='DescDataItem')
    for detail in details:
        if detail.find(class_='DescDataSubTit'):
            new_key = detail.find(class_='DescDataSubTit').get_text().strip().replace(' ','_')
            if new_key not in existing_columns:
                sql.pgsql_add_column('used_cars',new_key)
                existing_columns.append(new_key)
            car[detail.find(class_='DescDataSubTit').get_text().strip().lower()] = detail.find(class_='DescDataVal').get_text().strip()
    logging.info(car['make']," ", car['model'])
    sql.insert_pgsql_table_used('used_cars',car)

if __name__ == '__main__':
    sql.create_pgsql_table_new_cars_links()
    sql.create_pgsql_table_used_cars_links()
    get_links_new()
    get_links_used()
    extract_car_details_new(sql.read_link('new_cars_links',1))
    extract_car_details_used(sql.read_link('used_cars_links',1))
