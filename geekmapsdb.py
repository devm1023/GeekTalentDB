__all__ = [
    'Skill',
    'Title',
    'Company',
    'LIProfileSkill',
    'GeekMapsDB',
    ]

import conf
import numpy as np
import requests
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
    Float, \
    func
from geoalchemy2 import Geometry
from phrasematch import clean, stem, tokenize, matchStems


STR_MAX = 100000

SQLBase = sqlbase()


class Skill(SQLBase):
    __tablename__ = 'skill'
    nrmName   = Column(Unicode(STR_MAX), primary_key=True)
    name      = Column(Unicode(STR_MAX))
    count     = Column(BigInteger, index=True)

class Title(SQLBase):
    __tablename__ = 'title'
    nrmName          = Column(Unicode(STR_MAX), primary_key=True)
    name             = Column(Unicode(STR_MAX))
    count            = Column(BigInteger, index=True)

class Company(SQLBase):
    __tablename__ = 'company'
    nrmName          = Column(Unicode(STR_MAX), primary_key=True)
    name             = Column(Unicode(STR_MAX))
    count            = Column(BigInteger, index=True)

class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    id                = Column(BigInteger, primary_key=True)
    profileId         = Column(BigInteger, index=True)
    nrmSkill          = Column(Unicode(STR_MAX), index=True)
    location          = Column(Unicode(STR_MAX))
    nuts0             = Column(String(2), index=True)
    nuts1             = Column(String(3), index=True)
    nuts2             = Column(String(4), index=True)
    nuts3             = Column(String(5), index=True)
    nrmTitle          = Column(Unicode(STR_MAX), index=True)
    nrmCompany        = Column(Unicode(STR_MAX), index=True)
    rank              = Column(Float)
    indexedOn         = Column(Date, index=True)


    
class GeekMapsDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addSkill(self, nrmName, name, count):
        skill = self.query(Skill).filter(Skill.nrmName == nrmName) \
                                 .first()
        if not skill:
            skill = Skill(nrmName=nrmName)
            self.add(skill)
        skill.name = name
        skill.count = count
        return skill

    def addTitle(self, nrmName, name, count):
        title = self.query(Title).filter(Title.nrmName == nrmName) \
                                 .first()
        if not title:
            title = Title(nrmName=nrmName)
            self.add(title)
        title.name = name
        title.count = count
        return title

    def addCompany(self, nrmName, name, count):
        company = self.query(Company).filter(Company.nrmName == nrmName) \
                                     .first()
        if not company:
            company = Company(nrmName=nrmName)
            self.add(company)
        company.name = name
        company.count = count
        return company

    def addLIProfileSkill(self, profileId, location, nutsid,
                          nrmTitle, nrmCompany, nrmSkill,
                          rank, indexedOn):
        liprofileskill \
            = self.query(LIProfileSkill) \
                  .filter(LIProfileSkill.profileId == profileId,
                          LIProfileSkill.nrmSkill == nrmSkill) \
                  .first()
        if not liprofileskill:
            liprofileskill = LIProfileSkill(profileId=profileId,
                                            nrmSkill=nrmSkill)
            self.add(liprofileskill)
        if nutsid is None:
            nuts = [None]*4
        else:
            nuts = [nutsid[:i] for i in range(2, 6)]
        liprofileskill.location    = location
        liprofileskill.nuts0       = nuts[0]
        liprofileskill.nuts1       = nuts[1]
        liprofileskill.nuts2       = nuts[2]
        liprofileskill.nuts3       = nuts[3]
        liprofileskill.nrmTitle    = nrmTitle
        liprofileskill.nrmCompany  = nrmCompany
        liprofileskill.rank        = rank
        liprofileskill.indexedOn   = indexedOn

        return liprofileskill

