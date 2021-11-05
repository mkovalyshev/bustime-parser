from functions import *
from tqdm import tqdm

print("Start")  # LOG
print(YESTERDAY)

print("Creating table")

pg_engine.execute(f"""
-- DROP TABLE bustime.telemetry_{YESTERDAY.replace('-', '_')};

CREATE TABLE bustime.telemetry_{YESTERDAY.replace('-', '_')} (
	uniqueid varchar(8) NOT NULL,
	"timestamp" timestamp NOT NULL,
	bus_id int4 NOT NULL,
	heading int4 NULL,
	speed int4 NULL,
	lon float8 NOT NULL,
	lat float8 NOT NULL,
	direction int4 NULL,
	gosnum varchar(64) NULL,
	bortnum varchar(64) NULL,
	probeg int4 NULL,
	upload_date timestamp NOT NULL
);


-- bustime.telemetry_{YESTERDAY.replace('-', '_')} foreign keys

ALTER TABLE bustime.telemetry_{YESTERDAY.replace('-', '_')} ADD CONSTRAINT telemetry_fk FOREIGN KEY (bus_id) REFERENCES bustime.routes(id);
""")

print('Loading cities')

cities = get_cities()

print(f'LOG: fetched {len(list(cities.keys()))} cities')

for city in cities.keys():
    print(city)
    routes = get_routes(city, cities)

    for route in tqdm(routes):
        get_telemetry(city, route['id'])

