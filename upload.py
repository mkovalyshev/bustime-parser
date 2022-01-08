import os
import yaml
import pandas as pd
import migration as mig
import datetime
from sqlalchemy import create_engine
from utils import get_logger
from pandas.errors import EmptyDataError


def drop_constraints(engine_):
    """
    Drops all constraints from schema
    """

    logger_ = get_logger('drop_constraints')

    subqueries = []

    for relation in mig.CONSTRAINTS_NAMES.keys():
        logger_.debug(f'Found constraints in {relation}')
        for constraint in mig.CONSTRAINTS_NAMES[relation]:
            subqueries.append(f"ALTER TABLE transport.{relation} DROP CONSTRAINT IF EXISTS {constraint} CASCADE;")

    engine_.execute('\n'.join(subqueries))
    logger_.debug('Dropped constraints from schema')


def load_files(config_, engine_):
    """
    Loads .csv files from TEMP_FOLDER
    """

    logger_ = get_logger('upload')

    temp_files = os.listdir(config_['TEMP_FOLDER'])

    for file_ in temp_files:
        if os.path.isfile('/'.join([config_['TEMP_FOLDER'], file_])) and 'csv' in file_:
            logger_.debug(f'Processing {file_}')

            relation = file_.split('_')[0]
            df = pd.read_csv('/'.join([config_['TEMP_FOLDER'], file_]))

            engine_.execute(f"TRUNCATE TABLE transport.{relation};")
            logger_.debug(f'Truncated transport.{relation}')

            df.to_sql(relation,
                      engine_,
                      index=False,
                      if_exists='append',
                      schema='transport')

            logger_.debug(f'Loaded {len(df)} rows to transport.{relation}')

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


def upload_telemetry(config_, engine_):
    """
    uploads telemetry data to database
    """

    logger_ = get_logger('load_telemetry')

    temp_files = os.listdir(config_['TEMP_FOLDER'])

    temp_folders = []

    for file_ in temp_files:
        if not os.path.isfile('/'.join([config_['TEMP_FOLDER'], file_])):
            temp_folders.append(file_)

    logger_.debug(f'Will process these folders: {temp_folders}')
    logger_.debug('Looking for max date')

    date = engine_.execute("""
        select max(date("timestamp")) as max_date
        from transport.telemetry
        where True
    """).fetchone()[0]

    logger_.debug(f'Max date = {date}')

    for folder in sorted(temp_folders):
        logger_.debug(f'Processing {folder}')
        dfs = []
        for file_ in os.listdir(f'temp/{folder}'):
            if 'csv' in file_:
                try:
                    data = pd.read_csv(f'temp/{folder}/{file_}')
                    dfs.append(data)
                except EmptyDataError:
                    logger_.debug('Empty file')

        if len(dfs) == 0:
            logger_.debug('Dataframe is empty')
        else:
            df = pd.concat(dfs)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['upload_date'] = pd.to_datetime(df['upload_date'])

            dates = list(set(df['timestamp'].dt.date))

            logger_.debug(f'Found dates: {dates}')

            for date in dates:
                logger_.debug(f'Processing {date.strftime("%Y-%m-%d")}')
                df_date = df[df['timestamp'].dt.date == date]

                engine_.execute(mig.TELEMETRY_PARTITION_DDL.format(date.strftime('%Y_%m_%d'),
                                                                   date.strftime('%Y-%m-%d %H:%M:%S.%f'),
                                                                   (date + datetime.timedelta(days=1)). \
                                                                   strftime('%Y-%m-%d %H:%M:%S.%f')))

                logger_.debug('Loading...')

                df_date.to_sql('telemetry',
                               engine_,
                               index=False,
                               if_exists='append',
                               schema='transport')

                logger_.debug('Success')


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
