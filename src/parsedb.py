__all__ = [
    'LIProfile',
    'LIExperience',
    'LIEducation',
    'LIGroup',
    'LISkill',
    'ParseDB',
    'create_all',
    'drop_all',
    ]

import conf
from dbtools import *
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
    func
from sqlalchemy.orm import relationship

# character limit for string columns
STR_MAX = 100000

# base class for table objects
SQLBase = declarative_base()
# default database engine
engine = create_engine(conf.PARSE_DB)


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
    company_url   = Column(String(STR_MAX))
    location      = Column(Unicode(STR_MAX))
    current       = Column(Boolean)
    start         = Column(Unicode(STR_MAX))
    end           = Column(Unicode(STR_MAX))
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
    course        = Column(Unicode(STR_MAX))
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
    url           = Column(String(STR_MAX))


# database session class
ParseDB = sessionmaker(bind=engine)


def create_all():
    """Create all tables in `parse` database

    """
    SQLBase.metadata.create_all(engine)


def drop_all():
    """Drop all tables in `parse` database

    """
    SQLBase.metadata.drop_all(engine)

