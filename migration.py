import yaml
from utils import get_logger
from sqlalchemy import create_engine

SCHEMA_DDL = """
-- schema
CREATE SCHEMA IF NOT EXISTS transport AUTHORIZATION {};

-- cities relation
DROP TABLE IF EXISTS transport.cities CASCADE;

CREATE TABLE transport.cities (
    id     bigint NOT NULL CONSTRAINT cities_pk PRIMARY KEY
  , name   varchar(255) NOT NULL
  , CONSTRAINT unique_name UNIQUE(name)
);

-- routes relation
DROP TABLE IF EXISTS transport.routes CASCADE;

CREATE TABLE transport.routes (
    id        int NOT NULL CONSTRAINT routes_pk PRIMARY KEY
  , name      varchar(255) NOT NULL
  , type      varchar(255)
  , city_id   bigint NOT NULL REFERENCES transport.cities
);

COMMIT;
"""

CITIES_CONSTRAINTS = """
ALTER TABLE transport.cities ADD CONSTRAINT cities_pk PRIMARY KEY (id);
ALTER TABLE transport.cities ADD CONSTRAINT unique_name UNIQUE (name);
"""

ROUTES_CONSTRAINTS = """
ALTER TABLE transport.routes ADD CONSTRAINT routes_pk PRIMARY KEY (id);
ALTER TABLE transport.routes ADD CONSTRAINT routes_city_id_fkey FOREIGN KEY (city_id) REFERENCES transport.cities(id);
"""

CONSTRAINTS_DDL = {
    'cities': CITIES_CONSTRAINTS,
    'routes': ROUTES_CONSTRAINTS
}

CONSTRAINTS_NAMES = {
    'cities': ['cities_pk', 'unique_name'],
    'routes': ['routes_pk', 'routes_city_id_fkey']
}

TELEMETRY_DDL = """
CREATE TABLE IF NOT EXISTS transport.telemetry (
    uniqueid      varchar(8) NOT NULL
  , "timestamp"   timestamp NOT NULL
  , bus_id        bigint NOT NULL
  , heading       int
  , speed         int
  , lon           float NOT NULL
  , lat           float NOT NULL
  , direction     int
  , gosnum        varchar(255)
  , bortnum       varchar(255)
  , probeg        int
  , upload_date   timestamp NOT NULL
)
PARTITION BY RANGE("timestamp");
"""

TELEMETRY_PARTITION_DDL = """
CREATE TABLE transport.telemetry_{}
PARTITION OF transport.telemetry FOR VALUES FROM ('{}') TO ('{}');
"""


def run_migrations(query, config_):
    """
    Creates schema and relations (cities, routes)
    """

    logger_ = get_logger('migrations')

    postgres_engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(
        config_['DB_USER'],
        config_['DB_PASS'],
        config_['DB_HOST'],
        config_['DB_NAME']
    ))

    postgres_engine.execute(query.format(config_['DB_USER']))

    logger_.debug('Migration completed')

    return True


if __name__ == '__main__':

    with open('config.yaml') as file:
        config = yaml.Loader(file).get_data()

    logger = get_logger('main')

    if config['MIGRATION_COMPLETED']:
        logger.debug('Migration flag set to True, doing nothing')

    else:
        logger.debug('Migration flag set to False, doing migration')
        run_migrations(SCHEMA_DDL, config)