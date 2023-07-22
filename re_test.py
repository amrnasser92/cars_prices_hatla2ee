import requests
from bs4 import BeautifulSoup
url = 'https://eg.hatla2ee.com/en/new-car/mercedes/S-450'
r = requests.get(url)

soup = BeautifulSoup(r.text,'lxml')

h1 = soup.find('h1')

print(h1)