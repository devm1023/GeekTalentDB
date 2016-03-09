__all__ = [
    'Career',
    'CareerSkill',
    'CareerHackerDB',
    ]

import conf
from sqldb import *
from sqlalchemy import \
    Column, \
    ForeignKey, \
    UniqueConstraint, \
    Integer, \
    BigInteger, \
    Unicode, \
    UnicodeText, \
    String, \
    Text, \
    Date, \
    DateTime, \
    Float, \
    Boolean, \
    func, \
    or_
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry


STR_MAX = 100000

SQLBase = sqlbase()


class Career(SQLBase):
    __tablename__ = 'career'
    id            = Column(BigInteger, primay_key=True)
    name          = Column(Unicode(STR_MAX), index=True)
    sector        = Column(Unicode(STR_MAX), index=True)
    description   = Column(Unicode(STR_MAX))

    skills = relationship('CareerSkill',
                          order_by='CareerSkill.score',
                          cascade='all, delete-orphan')

class CareerSkill(SQLBase):
    __tablename__ = 'career_skill'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True)
    name          = Column(Unicode(STR_MAX), index=True)
    score         = Column(Float)
    

class CareerHackerDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)


