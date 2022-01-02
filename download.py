import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
from utils import sha256, get_logger


def get_cities(config_, file_=None) -> dict:
    """
    gets pandas.DataFrame of cities
    if file parameter = None returns a pandas.DataFrame, else writes to <file>.csv
    """

    soup = BeautifulSoup(requests.get(config_['HOST']).text, features="html.parser")

    cities = [x.get('href').strip('/') for x in soup.find("div", {"aria-label": " Список городов "}). \
        find_all("a", {"class": 'item'})]

    cities = [[sha256(city, 8), city] for city in cities]

    cities_df = pd.DataFrame(cities, columns=['id', 'name'])

    if not file_:
        return cities_df
    else:
        cities_df.to_csv(file_, encoding='utf-8', index=False)


def get_route_type(route):
    """
    returns type of route based on its name
    """

    if 'автобус' in route.lower():
        return 'bus'
    elif 'троллейбус' in route.lower():
        return 'trolley'
    elif 'трамвай' in route.lower():
        return 'tram'
    else:
        return 'other'


def get_routes(city: str, city_dict: dict, config_: dict, date: datetime.date, file_=None) -> list:
    """
    gets pandas.DataFrame of routes by city
    requires city_dict (id: name)
    """

    soup = BeautifulSoup(requests.get(config_['HOST'] + '/' + city + '/' + 'transport/' + date).text,
                         features="html.parser")

    routes = [
        [int(x.get('value')),
         x.text,
         get_route_type(x.text),
         city_dict[city]] for x in soup.find('select', {'name': 'bus_id'}). \
            find_all('option') if x.get('value') != '0']

    routes_df = pd.DataFrame(routes, columns=['id', 'name', 'type', 'city_id'])

    if not file_:
        return routes_df
    else:
        routes_df.to_csv(file_, encoding='utf-8', index=False)


def write_cities(config_):
    """
    Writes .csv with cities data to temp folder
    """

    filename = f'cities_{datetime.date.today().strftime("%Y-%m-%d").replace("-", "_")}.csv'
    get_cities(config_, '/'.join([config_['TEMP_FOLDER'], filename]))

    logger = get_logger('write_cities')
    logger.debug(f'Saved cities at {filename}')


def write_routes(config_, engine_):
    """
    Writes .csv with routes data to temp folder
    """

    logger = get_logger('write_routes')

    city_df = pd.read_sql("select id, name from transportation.cities", engine_)

    if len(city_df) == 0:
        logger.debug('Cities load from DB failed, fetching from HOST')
        city_df = get_cities(config_)

    city_dict = {x[1]: x[0] for x in city_df.to_records(index=False)}

    for city in config_['CITIES']:
        filename = f'routes_{datetime.date.today().strftime("%Y-%m-%d").replace("-", "_")}.csv'
        get_routes(city,
                   city_dict,
                   config_,
                   config_['DATE'].strftime("%Y-%m-%d"),
                   '/'.join([config_['TEMP_FOLDER'], filename]))


if __name__ == '__main__':
    ...  # TODO: make separate run possible
