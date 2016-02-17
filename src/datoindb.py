__all__ = [
    'LIProfile',
    'LIExperience',
    'LIEducation',
    'LIGroup',
    'INProfile',
    'INExperience',
    'INEducation',
    'INCertification',
    'UWProfile',
    'UWExperience',
    'UWEducation',
    'UWTest',
    'DatoinDB',
    ]

from sqldb import *
from sqlalchemy import \
    Column, \
    ForeignKey, \
    Integer, \
    BigInteger, \
    Unicode, \
    UnicodeText, \
    String, \
    Text, \
    Date, \
    Float, \
    func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ARRAY as Array


STR_MAX = 100000

SQLBase = sqlbase()

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(String(STR_MAX), primary_key=True)
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
    name              = Column(Unicode(STR_MAX))
    country           = Column(Unicode(STR_MAX))
    city              = Column(Unicode(STR_MAX))
    sector            = Column(Unicode(STR_MAX))
    title             = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    profileUrl        = Column(String(STR_MAX))
    profilePictureUrl = Column(String(STR_MAX))
    connections       = Column(String(STR_MAX))
    categories        = Column(Array(Unicode(STR_MAX)))
    indexedOn         = Column(BigInteger, index=True)
    crawledDate       = Column(BigInteger, index=True)

    experiences       = relationship('LIExperience',
                                     cascade='all, delete-orphan')
    educations        = relationship('LIEducation',
                                     cascade='all, delete-orphan')
    groups            = relationship('LIGroup',
                                     cascade='all, delete-orphan')
    
class LIExperience(SQLBase):
    __tablename__ = 'liexperience'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('liprofile.id'),
                         index=True)
    name        = Column(Unicode(STR_MAX))
    company     = Column(Unicode(STR_MAX))
    country     = Column(Unicode(STR_MAX))
    city        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))

class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('liprofile.id'),
                         index=True)
    institute   = Column(Unicode(STR_MAX))
    degree      = Column(Unicode(STR_MAX))
    area        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))

class LIGroup(SQLBase):
    __tablename__ = 'ligroup'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('liprofile.id'),
                         index=True)
    name        = Column(Unicode(STR_MAX))
    url         = Column(Unicode(STR_MAX))
    

class INProfile(SQLBase):
    __tablename__ = 'inprofile'
    id                = Column(String(STR_MAX), primary_key=True)
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
    name              = Column(Unicode(STR_MAX))
    country           = Column(Unicode(STR_MAX))
    city              = Column(Unicode(STR_MAX))
    title             = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    profileUrl        = Column(String(STR_MAX))
    indexedOn         = Column(BigInteger, index=True)
    crawledDate       = Column(BigInteger, index=True)

    experiences       = relationship('INExperience',
                                     cascade='all, delete-orphan')
    educations        = relationship('INEducation',
                                     cascade='all, delete-orphan')
    certifications    = relationship('INCertification',
                                     cascade='all, delete-orphan')
    
class INExperience(SQLBase):
    __tablename__ = 'inexperience'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('inprofile.id'),
                         index=True)
    name        = Column(Unicode(STR_MAX))
    company     = Column(Unicode(STR_MAX))
    country     = Column(Unicode(STR_MAX))
    city        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))

class INEducation(SQLBase):
    __tablename__ = 'ineducation'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('inprofile.id'),
                         index=True)
    institute   = Column(Unicode(STR_MAX))
    degree      = Column(Unicode(STR_MAX))
    area        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))

class INCertification(SQLBase):
    __tablename__ = 'incertification'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('inprofile.id'),
                         index=True)
    name        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))
    

class UWProfile(SQLBase):
    __tablename__ = 'uwprofile'
    id                = Column(String(STR_MAX), primary_key=True)
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
    name              = Column(Unicode(STR_MAX))
    country           = Column(Unicode(STR_MAX))
    city              = Column(Unicode(STR_MAX))
    title             = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    profileUrl        = Column(String(STR_MAX))
    profilePictureUrl = Column(String(STR_MAX))
    categories        = Column(Array(Unicode(STR_MAX)))
    indexedOn         = Column(BigInteger, index=True)
    crawledDate       = Column(BigInteger, index=True)

    experiences       = relationship('UWExperience',
                                     cascade='all, delete-orphan')
    educations        = relationship('UWEducation',
                                     cascade='all, delete-orphan')
    tests             = relationship('UWTest',
                                     cascade='all, delete-orphan')

class UWExperience(SQLBase):
    __tablename__ = 'uwexperience'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('uwprofile.id'),
                         index=True)
    name        = Column(Unicode(STR_MAX))
    company     = Column(Unicode(STR_MAX))
    country     = Column(Unicode(STR_MAX))
    city        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))

class UWEducation(SQLBase):
    __tablename__ = 'uweducation'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('uwprofile.id'),
                         index=True)
    institute   = Column(Unicode(STR_MAX))
    degree      = Column(Unicode(STR_MAX))
    area        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))

class UWTest(SQLBase):
    __tablename__ = 'uwtest'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('uwprofile.id'),
                         index=True)
    name        = Column(Unicode(STR_MAX))
    score       = Column(Float)
    
    
    
class DatoinDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)
    
