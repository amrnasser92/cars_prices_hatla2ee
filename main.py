import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import sqlalchemy
import asyncio
import psycopg2

base_url = 'https://eg.hatla2ee.com'
new_cars = []
used_cars = []
new_cars_links = []
used_cars_links = []

def get_links_new(page=1):
    print(f'Extracting details from page {page}')
    url =f'https://eg.hatla2ee.com/en/new-car/page/{page}'
    r = requests.get(url)
    soup = BeautifulSoup(r.text,'lxml')
    links =  soup.find_all('a',class_='nCarListData_title')
    for link in links:
        new_cars_links.append(base_url + link.attrs['href'])   
    next_page = soup.find_all('a',class_='paginate')[-1].attrs['href'].split("/")[-1]
    if int(next_page)>int(page):    
        get_links_new(next_page)    
    else:
        print('No more pages')
        print(f'Extracted {len(new_cars_links)} links')


def get_links_used(page=1):
    print(f'Extracting details from page {page}')
    url =f'https://eg.hatla2ee.com/en/car/page/{page}'
    r = requests.get(url)
    soup = BeautifulSoup(r.text,'lxml')
    links =  soup.find_all('a',class_='nCarListData_title')
    for link in links:
        print(link)
        used_cars_links.append(base_url + link.attrs['href'])   
    next_page = soup.find_all('a',class_='paginate')[-1].attrs['href'].split("/")[-1]
    if int(next_page)>int(page):    
        get_links_used(next_page)    
    else:
        print('No more pages')
        print(f'Extracted {len(used_cars_links)} links')


def get_car_detail(url:str)->BeautifulSoup:
    r = requests.get(url)
    print(url)
    return BeautifulSoup(r.text,'lxml')

def extract_car_details_new(soup:BeautifulSoup)->None:
    if 'used' in soup.find('h1',class_='mainTitle').get_text().lower():
        print('Used car')
        return
    tables = soup.find('div',class_='newCarPricesWrap').find_all('tbody')
    car_name = soup.find('h2',class_='brandCarTitle').get_text().strip()
    print(car_name)
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
                insert_pgsql_table(car)



def extract_car_details_used(soup:BeautifulSoup)->None:
    text = str(soup.find('div',class_='hidden-desktop UnitDescWhatsapp'))
    car = {
    'id': re.findall(r'\d+',text)[0],
    'price': int(soup.find('span',class_='usedUnitCarPrice').get_text().strip(' EGP').replace(',','')),
    'phone number': re.findall(r'\+\d+',text)[0]}
    if soup.find('strong',title="Installment"):
        car['installment'] = int(soup.find('strong',title="Installment").get_text().strip().strip(' EGP').replace(',',''))
        car['deposit'] = int(soup.find('strong',title="Deposit").get_text().strip(' EGP').replace(',',''))
    details = soup.find_all('div',class_='DescDataItem')
    for detail in details:
        if detail.find(class_='DescDataSubTit'):
            car[detail.find(class_='DescDataSubTit').get_text().strip().lower()] = detail.find(class_='DescDataVal').get_text().strip()
    used_cars.append(car)


def create_pgsql_table():
    conn = psycopg2.connect(database='cars',user='postgres',password='Amr')
    cursor=conn.cursor()
    
    SQL ="""

    CREATE TABLE IF NOT EXISTS new_cars(
        id VARCHAR(50),
        name VARCHAR(200),
        price INT,
        minimum_deposit INT,
        minimum_installment INT,
        CC VARCHAR(50),
        link VARCHAR(100),
        make VARCHAR(50),
        model VARCHAR(50)
    );

    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()


def read_pgsql_table():
    conn = psycopg2.connect(database='cars',user='postgres',password='Amr')
    cursor=conn.cursor()
    
    SQL ="""

    SELECT * FROM new_cars;
    """
    cursor.execute(SQL)
    result = cursor.fetchall()
    conn.close()
    print(result)


def insert_pgsql_table(data):
    conn = psycopg2.connect(database='cars',user='postgres',password='Amr')
    cursor=conn.cursor()
    values = tuple([data[column] for column in data])

    SQL =f"""

    INSERT INTO new_cars
    VALUES {values} 
    
    ;
    """
    print('Values Inserted')
    cursor.execute(SQL)
    conn.commit()
    conn.close()

def drop_pgsql_table():
    conn = psycopg2.connect(database='cars',user='postgres',password='Amr')
    cursor=conn.cursor()
    
    SQL =f"""

    DROP TABLE new_cars
    ;
    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    create_pgsql_table()
    get_links_new()
    for link in new_cars_links:
        extract_car_details_new(get_car_detail(link))
    pd.DataFrame(new_cars).to_csv('cars.csv')
    read_pgsql_table()


