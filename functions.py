import requests
import datetime as dt
from bs4 import BeautifulSoup
import yaml
from sqlalchemy import create_engine
import pandas as pd

with open('config.yaml') as f:
    CONFIG = yaml.load(f, Loader=yaml.FullLoader)

YESTERDAY = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d')
HOST = 'https://www.bustime.ru'
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,'
              'application/signed-exchange;v=b3;q=0.9',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/87.0.4280.141 Safari/537.36'
}

pg_engine = create_engine(f'postgresql+psycopg2://postgres:{CONFIG["db_pass"]}@localhost/postgres')


def get_cities(url: str = 'https://www.bustime.ru') -> dict:
    """
    gets dict of cities from database (if available)
    else scrapes from website and writes to db
    returns dict or None
    """

    cities = pg_engine.execute("""select * from bustime.cities
                                  where id in (7,21,28,46,54,5573,80,101,109,120,123,132,136,143)""").\
        fetchall()

    if len(cities) != 0:
        return dict(tuple([tuple([x[1], x[0]]) for x in cities]))

    else:

        soup = BeautifulSoup(requests.get(url).text, features="html.parser")

        cities = [x.get('href').strip('/') for x in soup.find("div", {"aria-label": " Список городов "}). \
            find_all("a", {"class": 'item'})]

        cities_df = pd.DataFrame(cities, columns=['name']).reset_index().rename(columns={'index': 'id'})

        cities_df.to_sql('cities',
                         pg_engine,
                         schema='bustime',
                         if_exists='append',
                         index=False)

        cities = pg_engine.execute("select * from bustime.cities").fetchall()

        return dict(tuple([tuple([x[1], x[0]]) for x in cities]))


def get_routes(city: str, cities_dict: dict) -> list:
    """
    gets list of dicts with routes data from database (if available)
    else scrapes from website and writes to db
    returns list of dicts
    """

    routes = pd.read_sql(f"select * from bustime.routes where city_id={cities_dict[city]}", pg_engine)

    if len(routes) != 0:
        routes_dict = routes.to_dict(orient='index')
        return [routes_dict[i] for i in routes_dict.keys()]

    else:
        soup = BeautifulSoup(requests.get(HOST + '/' + city + '/' + 'transport/' + YESTERDAY).text, features="html.parser")
        routes = {int(x.get('value')): x.text for x in soup.find('select', {'name': 'bus_id'}). \
            find_all('option') if x.get('value') != '0'}

        routes_df = pd.DataFrame(routes.items(), columns=['id', 'name'])
        routes_df['city_id'] = cities_dict[city]

        routes_df.to_sql('routes',
                         pg_engine,
                         schema='bustime',
                         if_exists='append',
                         index=False)

        routes = pd.read_sql(f"select * from bustime.routes where city_id={cities_dict[city]}", pg_engine)
        routes_dict = routes.to_dict(orient='index')
        return [routes_dict[i] for i in routes_dict.keys()]


def get_telemetry(city: str, bus_id: str = 0, date: str = YESTERDAY) -> None:
    """
    gets telemetry data from bustime.ru
    loads to database
    :param city: str  # city of search
    :param bus_id: str  # route id
    :param date: str  # date of search
    :return: None
    credit: github.com/az09
    """

    data = {'city_slug': city,
            'bus_id': bus_id,
            'day': date}

    response_df = pd.DataFrame(requests.post(HOST + '/ajax/transport/', data=data).json())
    if len(response_df)!=0:
        response_df['timestamp'] = date + ' ' + response_df['timestamp']
        response_df['timestamp'] = pd.to_datetime(response_df['timestamp'])
        response_df['upload_date'] = dt.datetime.today()

        response_df.to_sql(f'telemetry_{YESTERDAY.replace("-", "_")}',
                           pg_engine,
                           schema='bustime',
                           if_exists='append',
                           index=False)
