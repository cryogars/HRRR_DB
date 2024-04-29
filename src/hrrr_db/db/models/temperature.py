from hrrr_db.db import db_connection
from psycopg.rows import namedtuple_row


class Temperature:
    INSERT = "INSERT INTO temperatures(site_id, datetime, value) " \
             "VALUES (%(site_id)s, %(date)s::timestamptz, %(value)s)"

    SELECT = "SELECT datetime, value FROM temperatures " \
             "WHERE site_id = %(site_id)s"

    @classmethod
    def insert_pd(cls, site, dataframe):
        with db_connection() as (connection, cursor):
            with connection.transaction():
                for row in dataframe.itertuples():
                    cursor.execute(
                        cls.INSERT,
                        {
                            'site_id': site.id,
                            'date': row.Index,
                            'value': row.TMP
                        }
                    )

    @classmethod
    def load(cls, site_id):
        with db_connection() as (_connection, cursor):
            cursor.row_factory = namedtuple_row
            return cursor.execute(
                cls.SELECT,
                {'site_id': site_id}
            ).fetchall()

    @classmethod
    def as_pd(cls, site_id):
        pass
