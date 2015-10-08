__all__ = [
    'LIProfile',
    'Experience',
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
    country           = Column(Unicode(STR_MAX))
    city              = Column(Unicode(STR_MAX))
    title             = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    profileUrl        = Column(String(STR_MAX))
    profilePictureUrl = Column(String(STR_MAX))
    indexedOn         = Column(BigInteger)
    connections       = Column(String(STR_MAX))
    categories        = Column(Array(Unicode(STR_MAX)))

class Experience(SQLBase):
    __tablename__ = 'experience'
    id          = Column(String(STR_MAX), primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('liprofile.id'),
                         index=True)
    name        = Column(Unicode(STR_MAX))
    company     = Column(Unicode(STR_MAX))
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

    
class DatoinDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addLIProfile(self, profile, experiences, educations):
        # create or update LIProfile
        liprofile = self.query(LIProfile) \
                        .filter(LIProfile.id == profile['id']) \
                        .first()
        if not liprofile:
            new_profile = True
            liprofile = LIProfile(**profile)
            self.add(liprofile)
            self.flush()
        else:
            new_profile = False
            liprofile.parentId          = profile['parentId']
            liprofile.lastName          = profile['lastName']
            liprofile.firstName         = profile['firstName']
            liprofile.name              = profile['name']
            liprofile.country           = profile['country']
            liprofile.city              = profile['city']
            liprofile.title             = profile['title']
            liprofile.description       = profile['description']
            liprofile.profileUrl        = profile['profileUrl']
            liprofile.profilePictureUrl = profile['profilePictureUrl']
            liprofile.indexedOn         = profile['indexedOn']
            liprofile.connections       = profile['connections']
            liprofile.categories        = profile['categories']

        # add experiences
        if not new_profile:
            self.query(Experience) \
                .filter(Experience.parentId == liprofile.id) \
                .delete(synchronize_session='fetch')
        for experience in experiences:
            self.add(Experience(**experience))

        # add educations
        if not new_profile:
            self.query(Education) \
                .filter(Education.parentId == liprofile.id) \
                .delete(synchronize_session='fetch')
        for education in educations:
            self.add(Education(**education))

        self.flush()
        return liprofile
