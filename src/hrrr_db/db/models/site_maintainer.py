from enum import Enum, auto

from psycopg.types.enum import EnumInfo, register_enum


class SiteMaintainer(Enum):
    """
    Custom BSU enum for site maintainers.
    """
    NRCS = auto()
    BSU = auto()

    @classmethod
    def load_from_db(cls, connection):
        info = EnumInfo.fetch(connection, "site_maintainer")
        register_enum(info, connection, cls)
        return cls
