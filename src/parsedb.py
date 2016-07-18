__all__ = [
    'LIProfile',
    'LIExperience',
    'LIEducation',
    'LIGroup',
    'LISkill',
    'ParseDB',
    ]

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
    func
from sqlalchemy.orm import relationship


STR_MAX = 100000

SQLBase = sqlbase()

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id            = Column(BigInteger, primary_key=True)
    timestamp     = Column(DateTime, index=True, nullable=False)
    url           = Column(String(STR_MAX), nullable=False)
    picture_url   = Column(String(STR_MAX))
    name          = Column(Unicode(STR_MAX))
    location      = Column(Unicode(STR_MAX))
    sector        = Column(Unicode(STR_MAX))
    title         = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    connections   = Column(String(STR_MAX))

    experiences   = relationship('LIExperience',
                                 cascade='all, delete-orphan')
    educations    = relationship('LIEducation',
                                 cascade='all, delete-orphan')
    groups        = relationship('LIGroup',
                                 cascade='all, delete-orphan')
    skills        = relationship('LISkill',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('url'),)


class LIExperience(SQLBase):
    __tablename__ = 'liexperience'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           nullable=False,
                           index=True)
    title         = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    location      = Column(Unicode(STR_MAX))
    start         = Column(Date)
    end           = Column(Date)
    description   = Column(Unicode(STR_MAX))

class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           nullable=False,
                           index=True)
    institute     = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    subject       = Column(Unicode(STR_MAX))
    qualification = Column(Unicode(STR_MAX))
    start         = Column(BigInteger)
    end           = Column(BigInteger)
    description   = Column(Unicode(STR_MAX))

class LIGroup(SQLBase):
    __tablename__ = 'ligroup'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    url           = Column(Unicode(STR_MAX))

class LISkill(SQLBase):
    __tablename__ = 'liskill'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    url           = Column(Unicode(STR_MAX))


class ParseDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

