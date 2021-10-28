from functions import *

print("Start...", end='\n')  # LOG
print(TODAY, end='\n')

print("Loading cities.txt", end='\n')
if os.path.exists('resources/cities.txt'):
    print('LOG: found local file')
    with open('resources/cities.txt') as f:
        cities = f.read().split(';')
else:
    print('LOG: fetching from bustime.ru')
    get_cities()