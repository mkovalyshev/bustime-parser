import requests
import pandas as pd
import geopandas as gpd
from json import JSONDecodeError
from shapely.geometry import Polygon


def fetch_stops(city):
    """
    Returns df of bus stops from Overpass API
    """

    city = city[0].upper() + city[1:]

    query = f"""
    [out:json];

    area
      ["name:en"="{city}"];

    node
      [highway=bus_stop]
      (area);
    out body qt;
    """

    done_flag = False

    while not done_flag:
        try:
            response = requests.get("http://overpass-api.de/api/interpreter",
                                    params={'data': query.encode('utf-8')})
            df = pd.DataFrame(response.json()['elements'])
            done_flag = True
        except JSONDecodeError:
            pass

    df['geometry'] = gpd.GeoSeries.from_xy(x=df['lon'], y=df['lat'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    return gdf


def get_tags(data_raw, tags):
    """
    Returns normalized data from tags column
    """

    data = data_raw.copy()
    normalized = pd.json_normalize(data['tags'])

    data = data.join(normalized, how='left', rsuffix='_tag')

    data = data.drop(columns=['tags',
                              *[column for column in normalized.columns if column not in tags]
                              ])

    return data


def get_stops(city_name, tags, engine_):
    # TODO: insert logger here
    # TODO: write docstring

    stops_df = pd.DataFrame(get_tags(fetch_stops(city_name), tags)).drop(columns=['type', 'geometry'])

    city_id = engine_.execute(f"""
    select id
    from transport.cities
    where True
        and name = '{city_name}'
    """).fetchone()[0]

    utm = 32600 + int(stops_df['lon'].mean() + 186) // 6

    stops_df['city_id'] = city_id
    stops_df['utm'] = utm

    return stops_df[[
        'id',
        'lat',
        'lon',
        'utm',
        'name',
        'city_id'
    ]]


# get_stops('kazan', ['name'], postgres_engine)

def list_to_wkt(list_):
    list_ = [' '.join([str(coordinate) for coordinate in node]) for node in list_]
    wkt_string = 'LINESTRING (' + ','.join(list_) + ')'
    return wkt_string


def fetch_roads(city, clip=True):
    """
    Returns df with road graph
    """

    city = city[0].upper() + city[1:]

    nodes_query = f"""
    [out:json];
    area
      ["name:en"="{city}"];
    ( 
      node(area);
    );
    out body;
    >;
    out skel qt;
    """

    ways_query = f"""
    [out:json];
    area
      ["name:en"="{city}"];
    ( 
      way["highway"](area);
    );
    out body;
    >;
    out skel qt;
    """

    done_flag = False

    while not done_flag:
        try:
            ways_json = nodes_json = requests.get("http://overpass-api.de/api/interpreter",
                                                  params={'data': nodes_query.encode('utf-8')}).json()
            done_flag = True
        except JSONDecodeError:
            pass

    nodes_dict = {node['id']: [node['lon'], node['lat']] for node in nodes_json['elements']}

    done_flag = False

    while not done_flag:
        try:
            ways_json = requests.get("http://overpass-api.de/api/interpreter",
                                     params={'data': ways_query.encode('utf-8')}).json()
            done_flag = True
        except JSONDecodeError:
            pass

    for way in ways_json['elements']:
        if way.get('tags') is None:
            way['tags'] = {}

    ways_list = [{
        'id': way['id'],
        'nodes': way.get('nodes'),
        'name': way.get('tags').get('name'),
        'ref': way.get('tags').get('ref'),
        'highway': way.get('tags').get('highway'),
        'destination:ref': way.get('tags').get('destination:ref')
    } for way in ways_json['elements']]

    for way in ways_list:
        if way.get('nodes') is not None:
            node_coordinates = []
            for node in way['nodes']:
                node_get = nodes_dict.get(node)
                if node_get is not None:
                    node_coordinates.append(node_get)
            way['nodes_coordinates'] = node_coordinates

    df = pd.DataFrame(ways_list)
    df = df[df['nodes_coordinates'].notna()]
    df['wkt'] = df['nodes_coordinates'].apply(list_to_wkt)
    df['name'] = df['name'].combine_first(df['ref'].combine_first(df['destination:ref']))
    gdf = df.copy()[['id', 'name', 'highway', 'wkt']]
    gdf = gdf[(gdf['wkt'] != 'LINESTRING ()') & (gdf['wkt'].str.find(',') != -1)]
    gdf['geometry'] = gpd.GeoSeries.from_wkt(gdf['wkt'])
    gdf = gpd.GeoDataFrame(gdf, geometry='geometry')

    if clip:
        polygon_query = f"""
        [out:json][timeout:25];
        area
        ["name:en"="{city}"];
        (
          relation["boundary"]["admin_level"="6"](area);
        );
        out body;
        >;
        out skel qt;
        """

        done_flag = False

        while not done_flag:
            try:
                polygon_json = requests.get("http://overpass-api.de/api/interpreter",
                                            params={'data': polygon_query.encode('utf-8')}).json()
                done_flag = True
            except JSONDecodeError:
                pass

        admin_centre_id = [node for node in filter(lambda x: x['role'] == 'admin_centre',
                                                   polygon_json.get('elements')[0].get('members'))][0]['ref']
        polygon_df = pd.DataFrame([node for node in polygon_json['elements'] \
                                   if node.get('type') == 'node' and node.get('id') != admin_centre_id])
        polygon_gdf = gpd.GeoDataFrame(polygon_df,
                                       geometry=gpd.points_from_xy(polygon_df['lon'], polygon_df['lat']))

        polygon_gdf['coords'] = polygon_gdf['geometry'].apply(lambda x: list(x.coords)[0])

        polygon = Polygon(list(polygon_gdf['coords'])).convex_hull
        gdf = gpd.clip(gdf, polygon)

        gdf = gdf[(gdf['highway'].str.find('ary') != -1) |
                  (gdf['highway'].str.find('trunk') != -1) |
                  (gdf['highway'].str.find('residential') != -1) |
                  (gdf['highway'].str.find('living_street') != -1)]

    return gdf
