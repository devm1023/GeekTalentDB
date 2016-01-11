__all__ = [
    'LIProfile',
    'Experience',
    'Education',
    'Datoin2DB',
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
    Boolean, \
    func
from sqlalchemy.dialects.postgresql import ARRAY as Array


STR_MAX = 100000

SQLBase = sqlbase()

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(String(STR_MAX), primary_key=True)
    parentId          = Column(String(STR_MAX))
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
    name              = Column(Unicode(STR_MAX))
    isCompany         = Column(Boolean)
    country           = Column(Unicode(STR_MAX))
    city              = Column(Unicode(STR_MAX))
    sector            = Column(Unicode(STR_MAX))
    title             = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    profileUrl        = Column(String(STR_MAX))
    profilePictureUrl = Column(String(STR_MAX))
    indexedOn         = Column(BigInteger, index=True)
    crawledDate       = Column(BigInteger, index=True)
    connections       = Column(String(STR_MAX))
    categories        = Column(Array(Unicode(STR_MAX)))
    groups            = Column(Array(Unicode(STR_MAX)))

class Experience(SQLBase):
    __tablename__ = 'experience'
    id          = Column(String(STR_MAX), primary_key=True)
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
    indexedOn   = Column(BigInteger)

class Education(SQLBase):
    __tablename__ = 'education'
    id          = Column(String(STR_MAX), primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('liprofile.id'),
                         index=True)
    institute   = Column(Unicode(STR_MAX))
    degree      = Column(Unicode(STR_MAX))
    area        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))
    indexedOn   = Column(BigInteger)

    
class Datoin2DB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)
