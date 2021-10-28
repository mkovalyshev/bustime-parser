import requests
import datetime as dt
from bs4 import BeautifulSoup
import json
import os

TODAY = dt.datetime.today().strftime('%Y-%m-%d')
HOST = 'https://www.bustime.ru'
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
              'application/signed-exchange;v=b3;q=0.9',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/87.0.4280.141 Safari/537.36'
}


def get_cities(url: str = 'https://www.bustime.ru') -> None:
    """
    gets list of cities' hrefs from bustime.ru
    writes to resources/cities.txt
    returns None
    """

    soup = BeautifulSoup(requests.get(url).text)

    cities = [x.get('href') for x in soup.find("div", {"aria-label": " Список городов "}). \
        find_all("a", {"class": 'item'})]

    if not os.path.exists('resources/'):
        os.mkdir('resources/')

    with open("resources/cities.txt", "w") as file:
        file.write(';'.join(cities))


def get_routes(city: str) -> None:
    """
    gets dict of route ids matched with route names
    writes to resources/*city*/routes.json
    return None
    """

    soup = BeautifulSoup(requests.get(HOST + city + 'transport/' + TODAY).text)

    routes = {int(x.get('value')): x.text for x in soup.find('select', {'name': 'bus_id'}). \
        find_all('option') if x.get('value') != '0'}

    if not os.path.exists('resources/' + city.strip('/')):
        os.mkdir('resources/' + city.strip('/'))

    with open('resources/' + city.strip('/') + '/routes.json', 'w', encoding='utf-8') as file:
        json.dump(routes, file, ensure_ascii=False)


def post_ajax(city: str, bus_id: str = 0, date: str = TODAY) -> dict:
    """
    get point data from bustime.ru
    :param city: str  # city of search
    :param uid: str  # vehicle unique id
    :param bus_id: str  # route id
    :param date: str  # date of search
    :return: list  # with dicts of data
    credit: github.com/az09
    """

    data = {'city_slug': city,
            'bus_id': bus_id,
            'day': date}

    return requests.post(HOST + '/ajax/transport/', data=data).json()

