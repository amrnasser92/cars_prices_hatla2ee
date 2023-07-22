import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import sqlalchemy
import asyncio
import psycopg2
import httpx
import psycopg




base_url = 'https://eg.hatla2ee.com'
new_cars = []
used_cars = []
new_cars_links = []
used_cars_links = []
existing_columns = []

client = httpx.AsyncClient(timeout=None)


async def get_links_new_async(page=1):
    print(f'Extracting details from page {page}')
    url =f'https://eg.hatla2ee.com/en/new-car/page/{page}'

    r = await client.get(url)
    soup = BeautifulSoup(r.text,'lxml')
    links = soup.find_all('a',class_='nCarListData_title')
    for link in links:
        new_cars_links.append(base_url + link.attrs['href'])   
    # next_page = soup.find_all('a',class_='paginate')[-1].attrs['href'].split("/")[-1]
    # if int(next_page)>int(page):    
        # get_links_new(next_page)    
    # else:
        # print('No more pages')
        # print(f'Extracted {len(new_cars_links)} links')


async def get_last_page():
    url ='https://eg.hatla2ee.com/en/new-car/page/1'

    r = await client.get(url)
    soup = BeautifulSoup(r.text,'lxml')
    pagination = soup.find('div',class_='pagination')
    pages = pagination.find_all('a',class_='paginate')
    return max([int(page.get_text().strip()) for page in pages if 'Next' not in page.get_text().strip()] )


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
    cars = soup.find_all('div', class_='newCarListUnit_header')
    for car in cars:
        link = car.find('a')
        print(link.attrs['href'])
        #used_cars_links.append(base_url + link.attrs['href'])
        with open('usedcars.txt','a') as f:
            f.write(base_url +link.attrs['href']+'\n')   
    next_page = soup.find_all('a',class_='paginate')[-1].attrs['href'].split("/")[-1]
    if int(next_page)>int(page):    
        get_links_used(next_page)    
    else:
        print('No more pages')
        #print(f'Extracted {len(used_cars_links)} links')


async def get_car_detail(url:str)->BeautifulSoup:
    r = await client.get(url)
    print(url)
    return BeautifulSoup(r.text,'lxml')

async def extract_car_details_new(link:str)->None:
      try:
            soup = await get_car_detail(link)
            if not soup.find('h1'):
                  soup = await extract_car_details_new(link)
            if 'used' in soup.find('h1').get_text().lower():
                  print('Used car')
            tables = soup.find('div',class_='newCarPricesWrap').find_all('tbody')
            car_name = soup.find('h2',class_='brandCarTitle').get_text().strip()
            pattern = r'\d+,\d+'
            print(car_name)
            for table in tables:
                  models = table.find_all('tr')
                  for model in models:
                        details = model.find_all('td')
                  #   result = re.match(pattern, details[2].get_text().strip())
                  #   print(result)
                        if details:
                              car = {
                        'id': details[0].find('a').attrs['id'],
                        'name': details[0].get_text().strip(),
                        'price': int(details[1].get_text().strip().replace(' EGP','').replace(',','')) if not details[1].find('del') else int(details[1].find_all('strong')[-1].get_text().strip().replace(' EGP','').replace(',','')),
                        'minimum_deoposit': int(re.match(pattern, details[2].get_text().strip())[0].replace(',','')) if re.match(pattern, details[2].get_text().strip()) else 0,
                        'minimum_installment': int(details[3].get_text().strip().replace(' EGP','').replace(',','')),
                        'CC': details[4].get_text().strip(),
                        'link': details[0].find('a').attrs['href'],
                        'make': details[0].find('a').attrs['href'].split('/')[-3].title(),
                        'model': details[0].find('a').attrs['href'].split('/')[-2].title(),
                  }
                  # new_cars.append(car)
                              await insert_pgsql_table(car)
      except Exception as e:
           print(e)
        #    await extract_car_details_new(link)


async def extract_car_details_used(url:str)->None:
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
                pgsql_add_column('used_cars',new_key)
                existing_columns.append(new_key)
            car[detail.find(class_='DescDataSubTit').get_text().strip().lower()] = detail.find(class_='DescDataVal').get_text().strip()
    print(car['make']," ", car['model'])
    #used_cars.append(car)
    await insert_pgsql_table_used('used_cars',car)


async def create_pgsql_table_new_cars():
      async with await psycopg.AsyncConnection.connect("dbname=cars user=postgres password=Amr") as conn:
            async with conn.cursor() as cursor:
                  await cursor.execute(f"""

            CREATE TABLE IF NOT EXISTS new_cars_async(
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
                  )

def create_pgsql_table_used_cars():
    conn = psycopg2.connect(database='cars',user='postgres',password='Amr')
    cursor=conn.cursor()
    
    SQL =f"""

    CREATE TABLE IF NOT EXISTS used_cars(
        id VARCHAR(50),
        price INT,
        phone_number VARCHAR(20),
        installment INT,
        deposit INT
    );

    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()



def pgsql_add_column(table:str,column:str):
    conn = psycopg2.connect(database='cars',user='postgres',password='Amr')
    cursor=conn.cursor()
    
    SQL =f"""

    ALTER TABLE {table}
    ADD COLUMN IF NOT EXISTS {column} VARCHAR(50)  
    ;

    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()


def read_pgsql_table(table):
    conn = psycopg2.connect(database='cars',user='postgres',password='Amr')
    cursor=conn.cursor()
    
    SQL =f"""

    SELECT * FROM {table};
    """
    cursor.execute(SQL)
    result = cursor.fetchall()
    conn.close()
    print(result)


def len_pgsql_table(table):
    conn = psycopg2.connect(database='cars',user='postgres',password='Amr')
    cursor=conn.cursor()
    
    SQL =f"""

    SELECT COUNT(*) FROM {table};
    """
    cursor.execute(SQL)
    result = cursor.fetchall()
    conn.close()
    return result[0][0]


async def insert_pgsql_table(data):
      values = tuple([data[column] for column in data])
      async with await psycopg.AsyncConnection.connect("dbname=cars user=postgres password=Amr") as conn:
            async with conn.cursor() as cursor:
                  await cursor.execute(f"""

                  INSERT INTO new_cars_async
                        VALUES {values} ;
                        """)
                  


async def insert_pgsql_table_used(table:str,data:dict):
    conn = psycopg2.connect(database='cars',user='postgres',password='Amr')
    cursor=conn.cursor()
    columns = tuple(data.keys())
    values = tuple(data.values())

    SQL =f"""

    INSERT INTO {table} 
    ({','.join([column.strip().replace(' ','_') for column in columns])})
    VALUES {values} 
    
    ;
    """
    print('Values Inserted')
    cursor.execute(SQL)
    conn.commit()
    conn.close()


def drop_pgsql_table(table):
    conn = psycopg2.connect(database='cars',user='postgres',password='Amr')
    cursor=conn.cursor()
    
    SQL =f"""

    DROP TABLE {table}
    ;
    """
    cursor.execute(SQL)
    conn.commit()
    conn.close()


# def scrape_new():
#     get_links_new()
#     for link in new_cars_links:
#         extract_car_details_new(get_car_detail(link))
#     pd.DataFrame(new_cars).to_csv('new_cars.csv')


async def scrape_new():
    for link in new_cars_links:
      car_details = [get_car_detail(link) for link in new_cars_links ]
      return await asyncio.gather(extract_car_details_new(*car_details))
      # extract_car_details_new(get_car_detail(link))
      # pd.DataFrame(new_cars).to_csv('new_cars.csv')


# async def main():
#     saved =len_pgsql_table('used_cars')
#     with open('usedcars.txt','r') as f:
#         links = f.readlines()[saved+2:]
#     tasks = [await extract_car_details_used(link.strip()) for link in links]
#     asyncio.gather(*tasks)

async def main():
      await asyncio.gather(*[get_links_new_async(i) for i in range(1,await get_last_page() +1 )])
      await asyncio.gather(*[extract_car_details_new(link) for link in new_cars_links ] )
     

if __name__ == '__main__':
      asyncio.run(create_pgsql_table_new_cars())
      asyncio.run(main())
    #   print(len(new_cars_links))
    # asyncio.run(get_last_page())