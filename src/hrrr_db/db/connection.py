from contextlib import contextmanager
from pathlib import Path

import psycopg
from dotenv import dotenv_values
from psycopg.types import TypeInfo
from psycopg.types.shapely import register_shapely
from sqlalchemy import create_engine

HRRR_DB_ENV_FILE = Path.home().joinpath('.hrrr_db.env')
HRRR_DB_ENV = dotenv_values(HRRR_DB_ENV_FILE)

POSTGRESQL_CONNECTION = (f"{HRRR_DB_ENV['DB_USER']}:"
                         f"{HRRR_DB_ENV['DB_PASSWORD']}@"
                         f"{HRRR_DB_ENV['DB_HOST']}:"
                         f"{HRRR_DB_ENV['DB_PORT']}/"
                         f"{HRRR_DB_ENV['DB_NAME']}")
PSYCOPG_CONNECTION = f"postgresql://{POSTGRESQL_CONNECTION}"
SQLALCHEMY_CONNECTION = f"postgresql+psycopg://{POSTGRESQL_CONNECTION}"
SQLALCHEMY_ENGINE = create_engine(SQLALCHEMY_CONNECTION)


@contextmanager
def db_connection():
    with psycopg.connect(PSYCOPG_CONNECTION, autocommit=True) as connection:
        geom_info = TypeInfo.fetch(connection, "geometry")
        register_shapely(geom_info, connection)

        with connection.cursor() as cursor:
            yield connection, cursor


@contextmanager
def pd_db_connection():
    with SQLALCHEMY_ENGINE.connect() as connection:
        yield connection
