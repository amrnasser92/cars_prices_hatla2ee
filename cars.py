import requests
from bs4 import BeautifulSoup
import pandas as pd

base_url = 'https://eg.hatla2ee.com'


links_list = []


def get_links(page=1):
    print(f'Extracting details from page {page}')
    url =f'https://eg.hatla2ee.com/en/car/all-prices/page/{page}'
    r = requests.get(url)
    soup = BeautifulSoup(r.text,'lxml')
    links =  soup.select('td a')
    for link in links:
        if base_url + link.attrs['href'] in links_list:
            pass
        else:
            links_list.append(base_url + link.attrs['href'])

    
    next_page = soup.find_all('a',class_='paginate')[-1].attrs['href'].split("/")[-1]
    if int(next_page)>int(page):    
        get_links(next_page)    
    else:
        print('No more pages')
        print(f'Extracted {len(links_list)} links')




cars = {}


def car_detail(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text,'lxml')
    tables = soup.find_all('tbody')
    car_name = soup.find('h2',class_='brandCarTitle').get_text().strip()
    print(car_name)
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
                except:
                    cars[details[0].get_text().strip('\n','').strip()]['link'] = "N.A"
        except:
            pass

if __name__ == '__main__':
    get_links()
    error_links = []

    for link in links_list:
        try:
            car_detail(link)
        except:
            error_links.append(link)

cars = {}


pd.DataFrame(cars).transpose().to_csv('cars.csv')
