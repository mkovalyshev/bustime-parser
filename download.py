import os
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


def get_telemetry(date: datetime.date, city_name: str, route_id: int, config_, logger_, file_=None):
    """
    gets pandas.DataFrame of telemetry data by route_id
    """

    data = {
        'city_slug': city_name,
        'bus_id': str(route_id),
        'day': date.strftime('%Y-%m-%d')
    }

    telemetry_df = pd.DataFrame(requests.post(config_['HOST'] + '/ajax/transport/', data=data).json())

    if len(telemetry_df) != 0:
        telemetry_df['timestamp'] = date.strftime('%Y-%m-%d') + ' ' + telemetry_df['timestamp']
        telemetry_df['timestamp'] = pd.to_datetime(telemetry_df['timestamp'])
        telemetry_df['upload_date'] = datetime.datetime.today()

    logger_.debug(f'Date = {date} // City = {city_name} // Route = {route_id} // Row count = {len(telemetry_df)}')

    if not file_:
        return telemetry_df
    else:
        telemetry_df.to_csv(file_, encoding='utf-8', index=False)


def write_cities(config_):
    """
    Writes .csv with cities data to temp folder
    """

    filename = f'cities_{datetime.date.today().strftime("%Y_%m_%d")}.csv'
    get_cities(config_, '/'.join([config_['TEMP_FOLDER'], filename]))

    logger = get_logger('write_cities')
    logger.debug(f'Saved cities at {filename}')


def write_routes(config_, engine_):
    """
    Writes .csv with routes data to temp folder
    """

    logger = get_logger('write_routes')

    try:
        city_df = pd.read_sql("select id, name from transport.cities", engine_)
        logger.debug('Cities load from DB successful')
    except:
        city_df = pd.DataFrame([], columns=['id', 'name'])

    if len(city_df) == 0:
        logger.debug('Cities load from DB failed, fetching from HOST')
        city_df = get_cities(config_)

    city_dict = {x[1]: x[0] for x in city_df.to_records(index=False)}

    df_list = []

    for city in list(city_df['name']):
        df_list.append(get_routes(city,
                                  city_dict,
                                  config_,
                                  config_['DATE'].strftime("%Y-%m-%d")))

    filename = f'temp/routes_{datetime.date.today().strftime("%Y_%m_%d")}.csv'

    df = pd.concat(df_list)
    df.to_csv(filename, index=False)


def write_telemetry(date, city_name, route_id, config_, logger_):
    """
    Writes .csv with telemetry data into temp folder with subfolder
    """

    folder = f'telemetry_{date.strftime("%Y_%m_%d")}'
    path = '/'.join([config_['TEMP_FOLDER'], folder])

    if folder not in os.listdir(config_['TEMP_FOLDER']):
        os.mkdir(path)

    get_telemetry(date,
                  city_name,
                  route_id,
                  config_,
                  logger_,
                  path+f'/{city_name}_{route_id}_{date.strftime("%Y_%m_%d")}.csv')

    logger_.debug(f'Wrote data to {city_name}_{route_id}_{date.strftime("%Y_%m_%d")}.csv')
