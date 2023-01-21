import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging


BASE_URL:str = 'https://eg.hatla2ee.com'
links_list = []


def get_links(page:int=1)-> None:
    logging.info(f'Extracting details from page {page}')
    url =f'https://eg.hatla2ee.com/en/car/all-prices/page/{page}'
    r = requests.get(url)
    soup = BeautifulSoup(r.text,'lxml')
    links =  soup.select('td a')
    for link in links:
        links_list.append(BASE_URL + link.attrs['href'])   
    next_page = soup.find_all('a',class_='paginate')[-1].attrs['href'].split("/")[-1]
    if int(next_page)>int(page):    
        get_links(next_page)    
    else:
        logging.info('No more pages')
        logging.info(f'Extracted {len(links_list)} links')


def car_detail(url:str):
    r = requests.get(url)
    soup = BeautifulSoup(r.text,'lxml')
    tables = soup.find_all('tbody')
    car_name = soup.find('h2',class_='brandCarTitle').get_text().strip()
    logging.info(car_name)
    for table in tables:
        models = soup.find_all('tr')
        link_  = table.find('a').attrs['href']
        try:
            for mod in models:
                
                details = mod.find_all('td')
                cars[details[0].get_text().strip('\n','').strip()] = {'name' : car_name,
                'price' : details[1].get_text().strip('\n','').strip(',','').replace('EGP','').strip(),           
                'min_deposit': details[2].get_text().strip('\n','').strip(',','').replace('EGP','').strip(),
                'min_installment': details[3].get_text().strip('\n','').strip(',','').replace('EGP','').strip(),
                'cc': details[4].get_text().strip('\n','').strip(),
                }
                
                try:
                    cars[details[0].get_text().strip('\n','').strip()]['link'] = base_url+ link_
                except ValueError:
                    cars[details[0].get_text().strip('\n','').strip()]['link'] = "N.A"
        except TypeError:
            pass


if __name__ == '__main__':
    get_links()
    error_links = []
    for link in links_list:
        try:
            car_detail(link)
        except ValueError:
            error_links.append(link)


cars = {}


pd.DataFrame(cars).transpose().to_csv('cars.csv')
