__all__ = [
    'Skill',
    'Title',
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
