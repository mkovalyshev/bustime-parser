import os

from functions import *
from tqdm import tqdm

print("Start")  # LOG
print(TODAY)

print("Loading cities.txt")
if os.path.exists('resources/cities.txt'):
    print('LOG: found local file')
    with open('resources/cities.txt') as f:
        cities = f.read().split(';')
else:
    print('LOG: fetching from bustime.ru')
    get_cities()
    with open('resources/cities.txt') as f:
        cities = f.read().split(';')

print(f'LOG: fetched {len(cities)} cities')


print('Getting route lists')
for city in tqdm(cities):
    if not os.path.exists('resources'+city+'routes.json'):
        get_routes(city)
print('Success\n\n')

print('Getting telemetry')
for city in cities:
    with open('resources'+city+'routes.json', encoding='utf-8') as f:
        routes = json.load(f)

    print('\t', city)

    for route in routes.keys():  # Move with clause to function?
        directory = 'resources' + city + TODAY
        if not os.path.exists(directory):
            os.mkdir(directory)
        with open(directory+'/'+route+'.json', 'w', encoding='utf-8') as file:
            response = post_ajax(city, route, TODAY)
            json.dump(response, file, ensure_ascii=False)
