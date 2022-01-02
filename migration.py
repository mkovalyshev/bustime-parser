import yaml
from utils import get_logger
from sqlalchemy import create_engine

SCHEMA_DDL = """
-- schema
CREATE SCHEMA IF NOT EXISTS transportation AUTHORIZATION {};

-- cities relation
DROP TABLE IF EXISTS transportation.cities CASCADE;

CREATE TABLE transportation.cities (
    id     bigint NOT NULL CONSTRAINT cities_pk PRIMARY KEY
  , name   varchar(255) NOT NULL
  , CONSTRAINT unique_name UNIQUE(name)
);

-- routes relation
DROP TABLE IF EXISTS transportation.routes CASCADE;

CREATE TABLE transportation.routes (
    id        int NOT NULL CONSTRAINT routes_pk PRIMARY KEY
  , name      varchar(255) NOT NULL
  , type      varchar(255)
  , city_id   bigint NOT NULL REFERENCES transportation.cities
);
"""

CITIES_CONSTRAINTS = """
ALTER TABLE transportation.cities ADD CONSTRAINT cities_pk PRIMARY KEY (id);
ALTER TABLE transportation.cities ADD CONSTRAINT unique_name UNIQUE (name);
"""

ROUTES_CONSTRAINTS = """
ALTER TABLE transportation.routes ADD CONSTRAINT routes_pk PRIMARY KEY (id);
ALTER TABLE transportation.routes ADD CONSTRAINT routes_city_id_fkey FOREIGN KEY (city_id) REFERENCES transportation.cities(id);
"""

CONSTRAINTS_DDL = {
    'cities': CITIES_CONSTRAINTS,
    'routes': ROUTES_CONSTRAINTS
}

CONSTRAINTS_NAMES = {
    'cities': ['cities_pk', 'unique_name'],
    'routes': ['routes_pk', 'routes_city_id_fkey']
}


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
