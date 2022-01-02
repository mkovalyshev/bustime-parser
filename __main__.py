import os
import yaml
from sqlalchemy import create_engine
from utils import get_logger

logger = get_logger('__main__')

with open('config.yaml') as file:
    config = yaml.Loader(file).get_data()
    logger.debug('Config loaded')

postgres_engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(
    config['DB_USER'],
    config['DB_PASS'],
    config['DB_HOST'],
    config['DB_NAME']
))

logger.debug('Postgres engine created')

if config['TEMP_FOLDER'] not in os.listdir():
    logger.debug('TEMP_FOLDER not found')
    os.mkdir(config.get('TEMP_FOLDER'))
    logger.debug('TEMP_FOLDER created at', config.get('TEMP_FOLDER'))

if not config['MIGRATION_COMPLETED']:
    from migration import run_migrations, SCHEMA_DDL
    logger.debug('MIGRATION_COMPLETED flag set to False, running migrations')
    run_migrations(SCHEMA_DDL, config)

if config['UPDATE_CITIES']:
    from download import write_cities
    logger.debug('UPDATE_CITIES flag set to True, running write_cities')
    write_cities(config)

if config['UPDATE_ROUTES']:
    from download import write_routes
    logger.debug('UPDATE_ROUTES flag set to True, running write_routes')
    write_routes(config, postgres_engine)

if len(os.listdir(config['TEMP_FOLDER'])) != 0 and config['UPDATE_CITIES'] and config['UPDATE_ROUTES']:
    from upload import drop_constraints, set_constraints, load_files
    logger.debug('Found files at TEMP_FOLDER, writing to DB')
    drop_constraints(postgres_engine)
    load_files(config, postgres_engine)
    set_constraints(postgres_engine)
