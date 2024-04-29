from hrrr_db.db import db_connection
from psycopg.rows import namedtuple_row

from .site_maintainer import SiteMaintainer


class Site:
    INSERT = "INSERT INTO sites(name, maintainer, maintainer_id, geom) " \
             "VALUES (%(name)s, %(maintainer)s, %(maintainer_id)s, %(geom)s)"

    SELECT = "Select id, geom FROM sites WHERE name = %s"

    @classmethod
    def insert(cls, params):
        with db_connection() as (connection, cursor):
            site_maintainers = SiteMaintainer.load_from_db(connection)

            # Turn param into a recognized enum type
            params['maintainer'] = getattr(
                site_maintainers, params['maintainer']
            )

            with connection.transaction():
                cursor.execute(cls.INSERT, params)

    @classmethod
    def load(cls, name):
        with db_connection() as (_connection, cursor):
            cursor.row_factory = namedtuple_row
            return cursor.execute(
                cls.SELECT, (name,)
            ).fetchone()
