from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Reflected(DeferredReflection):
    __abstract__ = True
