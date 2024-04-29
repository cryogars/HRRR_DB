from geoalchemy2 import Geometry  ## noqa

from hrrr_db.db.connection import SQLALCHEMY_ENGINE
from .base import Base, Reflected


class Radiation(Reflected, Base):
    __tablename__ = 'radiation'

    __mapper_args__ = {"primary_key": ["site_id", "data_source", "datetime"]}


Radiation.prepare(SQLALCHEMY_ENGINE)
