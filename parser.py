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
    get_routes(city)
print('Success\n\n')

print('Getting telemetry')
