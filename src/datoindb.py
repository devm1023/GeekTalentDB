__all__ = [
    'LIProfile',
    'LIExperience',
    'LIEducation',
    'INProfile',
    'INExperience',
    'INEducation',
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

class LIExperience(SQLBase):
    __tablename__ = 'liexperience'
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

class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
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


class INProfile(SQLBase):
    __tablename__ = 'inprofile'
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
    indexedOn         = Column(BigInteger, index=True)
    crawledDate       = Column(BigInteger, index=True)

class INExperience(SQLBase):
    __tablename__ = 'inexperience'
    id          = Column(String(STR_MAX), primary_key=True)
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
    indexedOn   = Column(BigInteger)

class INEducation(SQLBase):
    __tablename__ = 'ineducation'
    id          = Column(String(STR_MAX), primary_key=True)
    parentId    = Column(String(STR_MAX),
                         ForeignKey('inprofile.id'),
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

    def addLIProfile(self, liprofiledict):
        liexperiences = liprofiledict.pop('experiences')
        lieducations = liprofiledict.pop('educations')
        
        # create or update LIProfile
        liprofile = self.query(LIProfile) \
                        .filter(LIProfile.id == liprofiledict['id']) \
                        .first()
        if not liprofile:
            new_profile = True
            liprofile = LIProfile(**liprofiledict)
            self.add(liprofile)
            self.flush()
        elif liprofiledict['indexedOn'] >= liprofile.indexedOn:
            new_profile = False
            for key, val in liprofiledict.items():
                setattr(liprofile, key, val)
        else:
            return liprofile

        # add liexperiences
        if not new_profile:
            self.query(LIExperience) \
                .filter(LIExperience.parentId == liprofile.id) \
                .delete(synchronize_session='fetch')
        idset = set()
        for liexperience in liexperiences:
            if liexperience['id'] not in idset:
                idset.add(liexperience['id'])
                self.add(LIExperience(**liexperience))

        # add lieducations
        if not new_profile:
            self.query(LIEducation) \
                .filter(LIEducation.parentId == liprofile.id) \
                .delete(synchronize_session='fetch')
        idset = set()
        for lieducation in lieducations:
            if lieducation['id'] not in idset:
                idset.add(lieducation['id'])
                self.add(LIEducation(**lieducation))

        self.flush()
        return liprofile

    def addINProfile(self, inprofiledict):
        inexperiences = inprofiledict.pop('experiences')
        ineducations = inprofiledict.pop('educations')
        
        # create or update INProfile
        inprofile = self.query(INProfile) \
                        .filter(INProfile.id == inprofiledict['id']) \
                        .first()
        if not inprofile:
            new_profile = True
            inprofile = INProfile(**inprofiledict)
            self.add(inprofile)
            self.flush()
        elif inprofiledict['indexedOn'] >= inprofile.indexedOn:
            new_profile = False
            for key, val in inprofiledict.items():
                setattr(inprofile, key, val)
        else:
            return inprofile

        # add inexperiences
        if not new_profile:
            self.query(INExperience) \
                .filter(INExperience.parentId == inprofile.id) \
                .delete(synchronize_session='fetch')
        idset = set()
        for liexperience in inexperiences:
            if liexperience['id'] not in idset:
                idset.add(liexperience['id'])
                self.add(INExperience(**liexperience))

        # add ineducations
        if not new_profile:
            self.query(INEducation) \
                .filter(INEducation.parentId == inprofile.id) \
                .delete(synchronize_session='fetch')
        idset = set()
        for lieducation in ineducations:
            if lieducation['id'] not in idset:
                idset.add(lieducation['id'])
                self.add(INEducation(**lieducation))

        self.flush()
        return inprofile
    
