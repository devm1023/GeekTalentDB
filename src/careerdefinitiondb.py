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
    careerName    = Column(Unicode(STR_MAX), index=True)
    linkedinSector = Column(Unicode(STR_MAX), index=True)
    description   = Column(Unicode(STR_MAX))
    totalCount    = Column(BigInteger)
    sectorCount   = Column(BigInteger)
    careerCount   = Column(BigInteger)
    careerSectorCount = Column(BigInteger)
    relevanceScore = Column(Float)

    skillCloud = relationship('CareerSkill',
                              order_by='CareerSkill.relevanceScore',
                              cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('linkedinSector', 'careerName'),)

class CareerSkill(SQLBase):
    __tablename__ = 'career_skill'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True)
    skillName     = Column(Unicode(STR_MAX), index=True)
    description   = Column(Unicode(STR_MAX))
    totalCount    = Column(BigInteger)
    careerCount   = Column(BigInteger)
    skillCount    = Column(BigInteger)
    skillCareerCount = Column(BigInteger)
    relevanceScore = Column(Float)
    
    __table_args__ = (UniqueConstraint('careerId', 'skillName'),)

class CareerDefinitionDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addCareer(self, careerdict, update=False):
        id = self.query(Career.id) \
                 .filter(Career.linkedinSector \
                         == careerdict['linkedinSector'],
                         Career.careerName \
                         == careerdict['careerName']).first()
        if id is not None:
            if not update:
                return None
            else:
                id = id[0]
                careerdict['id'] = id

        for skill in careerdict.get('skillCloud', []):
            skillid = self.query(CareerSkill.id) \
                          .filter(CareerSkill.careerId == id,
                                  CareerSkill.skillName \
                                  == skill.get('skillName', None)) \
                          .first()
            if skillid is not None:
                skill['id'] = skillid[0]
                skill['careerId'] = id

        return self.addFromDict(careerdict, Career)

    def getCareers(self, sectors, titles):
        results = []
        q = self.query(Career)
        if sectors:
            q = q.filter(Career.linkedinSector.in_(sectors))
        if titles:
            q = q.filter(Career.careerName.in_(titles))
        for career in q:
            careerdict = dictFromRow(career)
            careerdict.pop('id')
            for skilldict in careerdict['skillCloud']:
                skilldict.pop('id')
                skilldict.pop('careerId')
            results.append(careerdict)

        return results
