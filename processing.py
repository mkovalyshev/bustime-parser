import datetime
import requests
import pandas as pd
import yaml
from sqlalchemy import create_engine
import geopandas as gpd
from functions import YESTERDAY

with open('config.yaml') as f:
    CONFIG = yaml.load(f, Loader=yaml.FullLoader)

pg_engine = create_engine(f'postgresql+psycopg2://postgres:{CONFIG["db_pass"]}@localhost/postgres')


def get_data(date: str, city: str, threshold: int) -> pd.DataFrame:
    """
    Returns a pandas.DataFrame of telemetry data for certain city within certain date. Filters data by speed using
    passed threshold
    """

    query = f"""
    select t.*,
           uniqueid || '_' || to_char(timestamp, 'MM_DD_HH_MI_SS') as point_id,
           row_number() over (partition by uniqueid order by timestamp) as ordinal
    from bustime.telemetry_{date} t
        left join bustime.routes r on t.bus_id = r.id
        left join bustime.cities c on c.id = r.city_id
    where True
        and c.name = '{city}'
        and speed <= {threshold}
    """

    return pd.read_sql(query, pg_engine)


def get_stops(city: str) -> pd.DataFrame:
    """
    Returns a pandas.DataFrame object containing nodes of highway=bus_stop within certain city
    """

    city = city[0].upper() + city[1:].lower()

    overpass_query = f"""
    [out:json];

    area
      ["name:en"="{city}"];

    node
      [highway=bus_stop]
      (area);
    out body qt;
    """

    response = requests.get("http://overpass-api.de/api/interpreter",
                            params={'data': overpass_query.encode('utf-8')})

    stops = pd.DataFrame(response.json()['elements'])

    return stops


def project_df(df: pd.DataFrame, utm: int, lat_col='lat', lon_col='lon') -> gpd.GeoDataFrame:
    """
    Transforms pandas.DataFrame with lat/lon coordinates in EPSG:4326 to a geopandas.GeoDataFrame projected in UTM
    projection with passed code
    """

    gdf = gpd.GeoDataFrame(df,
                           geometry=gpd.points_from_xy(df[lon_col], df[lat_col]))

    gdf = gdf.set_crs('epsg:4326').to_crs(f'epsg:{utm}')

    return gdf


def buffer_clip(to_clip: gpd.GeoDataFrame, mask: gpd.GeoDataFrame, buffer_size: int):
    """
    Clips gdf passed in 'to_clip' arg using gdf passed in 'mask' arg over buffers with 'buffer_size' size
    """

    mask['buffer'] = mask['geometry'].buffer(buffer_size)

    clipped = gpd.clip(to_clip, mask['buffer'])

    return clipped


data = get_data(YESTERDAY.replace('-', '_'), 'kazan', 50)

stops = get_stops('kazan')

data_utm = project_df(data, 32639)
stops_utm = project_df(stops, 32639)

data_clipped = buffer_clip(data_utm, stops_utm, 50)

data_clipped_sjoin = data_clipped.sjoin_nearest(stops_utm)[[
    'uniqueid',
    'timestamp',
    'bus_id',
    'heading',
    'speed',
    'lon_left',
    'lat_left',
    'direction',
    'gosnum',
    'bortnum',
    'probeg',
    'upload_date',
    'point_id',
    'ordinal',
    'geometry',
    'id'
]]

data_clipped_sjoin = data_clipped_sjoin.rename(columns={'id': 'nearest_stop_id'})



...

# TODO: Implement loading to database
