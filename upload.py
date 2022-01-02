import os
import yaml
import pandas as pd
import migration as mig
from sqlalchemy import create_engine
from utils import get_logger


def drop_constraints(engine_):
    """
    Drops all constraints from schema
    """

    logger_ = get_logger('drop_constraints')

    subqueries = []

    for relation in mig.CONSTRAINTS_NAMES.keys():
        logger_.debug(f'Found constraints in {relation}')
        for constraint in mig.CONSTRAINTS_NAMES[relation]:
            subqueries.append(f"ALTER TABLE transportation.{relation} DROP CONSTRAINT IF EXISTS {constraint} CASCADE;")

    engine_.execute('\n'.join(subqueries))
    logger_.debug('Dropped constraints from schema')


def load_files(config_, engine_):
    """
    Loads .csv files from TEMP_FOLDER
    """

    logger_ = get_logger('upload')

    temp_files = os.listdir(config_['TEMP_FOLDER'])

    for file_ in temp_files:
        logger_.debug(f'Processing {file_}')

        relation = file_.split('_')[0]
        df = pd.read_csv('/'.join([config_['TEMP_FOLDER'], file_]))

        engine_.execute(f"TRUNCATE TABLE transportation.{relation};")
        logger_.debug(f'Truncated transportation.{relation}')

        df.to_sql(relation,
                  engine_,
                  index=False,
                  if_exists='append',
                  schema='transportation')

        logger_.debug(f'Loaded {len(df)} rows to transportation.{relation}')

        if config_['REMOVE_TEMP']:
            os.remove('/'.join([config_['TEMP_FOLDER'], file_]))
            logger_.debug(f'Removed {file_}')


def set_constraints(engine_):
    """
    Sets constraints on schema
    """

    logger_ = get_logger('set_constraints')

    for relation in mig.CONSTRAINTS_DDL.keys():
        engine_.execute(mig.CONSTRAINTS_DDL[relation])
        logger_.debug(f'Set constraints on {relation}')


if __name__ == '__main__':

    with open('config.yaml') as file:
        config = yaml.Loader(file).get_data()

    postgres_engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(
        config['DB_USER'],
        config['DB_PASS'],
        config['DB_HOST'],
        config['DB_NAME']
    ))

    drop_constraints(postgres_engine)
    load_files(config, postgres_engine)
    set_constraints(postgres_engine)
