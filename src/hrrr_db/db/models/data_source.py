from enum import Enum, auto

from psycopg.types.enum import EnumInfo, register_enum


class DataSource(Enum):
    """
    Source of stored data in the DB.

    Use this class for direct access via psycopg.
    """
    NRCS = auto()
    HRRR = auto()

    @classmethod
    def load_from_db(cls, connection):
        info = EnumInfo.fetch(connection, "data_source")
        register_enum(info, connection, cls)
        return cls
