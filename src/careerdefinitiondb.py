__all__ = [
    'Career',
    'CareerSkill',
    'CareerDefinitionDB',
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
from pprint import pprint


STR_MAX = 100000

SQLBase = sqlbase()


class Career(SQLBase):
    __tablename__ = 'career'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX), index=True)
    sector        = Column(Unicode(STR_MAX), index=True)
    description   = Column(Unicode(STR_MAX))
    totalCount    = Column(BigInteger)
    sectorCount   = Column(BigInteger)
    careerCount   = Column(BigInteger)
    count         = Column(BigInteger)
    score         = Column(Float)

    skills = relationship('CareerSkill',
                          order_by='CareerSkill.score',
                          cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('sector', 'name'),)

class CareerSkill(SQLBase):
    __tablename__ = 'career_skill'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True)
    name          = Column(Unicode(STR_MAX), index=True)
    description   = Column(Unicode(STR_MAX))
    totalCount    = Column(BigInteger)
    careerCount   = Column(BigInteger)
    skillCount    = Column(BigInteger)
    count         = Column(BigInteger)
    score         = Column(Float)
    
    __table_args__ = (UniqueConstraint('careerId', 'name'),)

class CareerDefinitionDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addCareer(self, careerdict, update=False):
        id = self.query(Career.id) \
                 .filter(Career.sector == careerdict['sector'],
                         Career.name == careerdict['name']).first()
        if id is not None:
            if not update:
                return None
            else:
                id = id[0]
                careerdict['id'] = id

        for skill in careerdict.get('skills', []):
            skillid = self.query(CareerSkill.id) \
                          .filter(CareerSkill.careerId == id,
                                  CareerSkill.name == skill.get('name', None)) \
                          .first()
            if skillid is not None:
                skill['id'] = skillid[0]
                skill['careerId'] = id

        return self.addFromDict(careerdict, Career)
