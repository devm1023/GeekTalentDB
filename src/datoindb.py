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
    'MUProfile',
    'MUGroup',
    'MUEvent',
    'MUComment',
    'MULink',
    'GHProfile',
    'GHLink',
    'DatoinDB',
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
    Float, \
    func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ARRAY as Array


STR_MAX = 100000

SQLBase = sqlbase()

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(BigInteger, primary_key=True)
    profileId         = Column(String(STR_MAX), index=True, nullable=False)
    crawlNumber       = Column(BigInteger, index=True, nullable=False)
    name              = Column(Unicode(STR_MAX))
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
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
    crawlFailCount    = Column(BigInteger, index=True)

    experiences       = relationship('LIExperience',
                                     cascade='all, delete-orphan')
    educations        = relationship('LIEducation',
                                     cascade='all, delete-orphan')
    groups            = relationship('LIGroup',
                                     cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('profileId', 'crawlNumber'),)
    
class LIExperience(SQLBase):
    __tablename__ = 'liexperience'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         nullable=False,
                         index=True)
    name        = Column(Unicode(STR_MAX))
    company     = Column(Unicode(STR_MAX))
    url         = Column(String(STR_MAX))
    country     = Column(Unicode(STR_MAX))
    city        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))

class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         nullable=False,
                         index=True)
    name        = Column(Unicode(STR_MAX))
    url         = Column(String(STR_MAX))
    degree      = Column(Unicode(STR_MAX))
    area        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))

class LIGroup(SQLBase):
    __tablename__ = 'ligroup'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         nullable=False,
                         index=True)
    name        = Column(Unicode(STR_MAX))
    url         = Column(Unicode(STR_MAX))
    

class INProfile(SQLBase):
    __tablename__ = 'inprofile'
    id                = Column(BigInteger, primary_key=True)
    profileId         = Column(String(STR_MAX), index=True, nullable=False)
    crawlNumber       = Column(BigInteger, index=True, nullable=False)
    name              = Column(Unicode(STR_MAX))
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
    country           = Column(Unicode(STR_MAX))
    city              = Column(Unicode(STR_MAX))
    title             = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    additionalInformation = Column(Unicode(STR_MAX))
    profileUrl        = Column(String(STR_MAX))
    profileUpdatedDate = Column(BigInteger)
    indexedOn         = Column(BigInteger, index=True)
    crawledDate       = Column(BigInteger, index=True)
    crawlFailCount    = Column(BigInteger, index=True)

    experiences       = relationship('INExperience',
                                     cascade='all, delete-orphan')
    educations        = relationship('INEducation',
                                     cascade='all, delete-orphan')
    certifications    = relationship('INCertification',
                                     cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('profileId', 'crawlNumber'),)
    
class INExperience(SQLBase):
    __tablename__ = 'inexperience'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(BigInteger,
                         ForeignKey('inprofile.id'),
                         nullable=False,
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
    parentId    = Column(BigInteger,
                         ForeignKey('inprofile.id'),
                         nullable=False,
                         index=True)
    name        = Column(Unicode(STR_MAX))
    degree      = Column(Unicode(STR_MAX))
    area        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))

class INCertification(SQLBase):
    __tablename__ = 'incertification'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(BigInteger,
                         ForeignKey('inprofile.id'),
                         nullable=False,
                         index=True)
    name        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))
    

class UWProfile(SQLBase):
    __tablename__ = 'uwprofile'
    id                = Column(BigInteger, primary_key=True)
    profileId         = Column(String(STR_MAX), index=True, nullable=False)
    crawlNumber       = Column(BigInteger, index=True, nullable=False)
    name              = Column(Unicode(STR_MAX))
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
    country           = Column(Unicode(STR_MAX))
    city              = Column(Unicode(STR_MAX))
    title             = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    profileUrl        = Column(String(STR_MAX))
    profilePictureUrl = Column(String(STR_MAX))
    categories        = Column(Array(Unicode(STR_MAX)))
    indexedOn         = Column(BigInteger, index=True)
    crawledDate       = Column(BigInteger, index=True)
    crawlFailCount    = Column(BigInteger, index=True)

    experiences       = relationship('UWExperience',
                                     cascade='all, delete-orphan')
    educations        = relationship('UWEducation',
                                     cascade='all, delete-orphan')
    tests             = relationship('UWTest',
                                     cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('profileId', 'crawlNumber'),)
    
class UWExperience(SQLBase):
    __tablename__ = 'uwexperience'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(BigInteger,
                         ForeignKey('uwprofile.id'),
                         nullable=False,
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
    parentId    = Column(BigInteger,
                         ForeignKey('uwprofile.id'),
                         nullable=False,
                         index=True)
    name        = Column(Unicode(STR_MAX))
    degree      = Column(Unicode(STR_MAX))
    area        = Column(Unicode(STR_MAX))
    dateFrom    = Column(BigInteger)
    dateTo      = Column(BigInteger)
    description = Column(Unicode(STR_MAX))

class UWTest(SQLBase):
    __tablename__ = 'uwtest'
    id          = Column(BigInteger, primary_key=True)
    parentId    = Column(BigInteger,
                         ForeignKey('uwprofile.id'),
                         nullable=False,
                         index=True)
    name        = Column(Unicode(STR_MAX))
    score       = Column(Float)
    testPercentile = Column(Float)
    testDate    = Column(BigInteger)
    testDuration = Column(Float)

class MUProfile(SQLBase):
    __tablename__ = 'muprofile'
    id                = Column(BigInteger, primary_key=True)
    profileId         = Column(String(STR_MAX), index=True, nullable=False)
    crawlNumber       = Column(BigInteger, index=True, nullable=False)
    name                   = Column(Unicode(STR_MAX))
    country                = Column(Unicode(STR_MAX))
    city                   = Column(Unicode(STR_MAX))
    latitude               = Column(Float)
    longitude              = Column(Float)
    status                 = Column(Unicode(STR_MAX))
    description            = Column(Unicode(STR_MAX))
    profileUrl             = Column(String(STR_MAX))
    profilePictureId       = Column(String(STR_MAX))
    profilePictureUrl      = Column(String(STR_MAX))
    profileHQPictureUrl    = Column(String(STR_MAX))
    profileThumbPictureUrl = Column(String(STR_MAX))
    categories             = Column(Array(Unicode(STR_MAX)))
    indexedOn              = Column(BigInteger, index=True)
    crawledDate            = Column(BigInteger, index=True)
    crawlFailCount    = Column(BigInteger, index=True)
    
    groups                 = relationship('MUGroup',
                                          cascade='all, delete-orphan')
    events                 = relationship('MUEvent',
                                          cascade='all, delete-orphan')
    comments               = relationship('MUComment',
                                          cascade='all, delete-orphan')
    links                  = relationship('MULink',
                                          cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('profileId', 'crawlNumber'),)

class MUGroup(SQLBase):
    __tablename__ = 'mugroup'
    id            = Column(BigInteger, primary_key=True)
    parentId      = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           nullable=False,
                           index=True)
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    latitude      = Column(Float)
    longitude     = Column(Float)
    timezone      = Column(Unicode(STR_MAX))
    utcOffset     = Column(Integer)
    name          = Column(Unicode(STR_MAX))
    categoryName  = Column(Unicode(STR_MAX))
    categoryShortname = Column(Unicode(STR_MAX))
    categoryId    = Column(String(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    urlname       = Column(String(STR_MAX))
    pictureUrl    = Column(String(STR_MAX))
    pictureId     = Column(BigInteger)
    HQPictureUrl  = Column(String(STR_MAX))
    thumbPictureUrl = Column(String(STR_MAX))
    joinMode      = Column(Unicode(STR_MAX))
    rating        = Column(Float)
    organizerName = Column(Unicode(STR_MAX))
    organizerId   = Column(String(STR_MAX))
    members       = Column(Integer)
    state         = Column(Unicode(STR_MAX))
    visibility    = Column(Unicode(STR_MAX))
    who           = Column(Unicode(STR_MAX))
    categories    = Column(Array(Unicode(STR_MAX)))
    createdDate   = Column(BigInteger)

class MUEvent(SQLBase):
    __tablename__ = 'muevent'
    id            = Column(BigInteger, primary_key=True)
    parentId      = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           nullable=False,
                           index=True)
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    addressLine1  = Column(Unicode(STR_MAX))
    addressLine2  = Column(Unicode(STR_MAX))
    latitude      = Column(Float)
    longitude     = Column(Float)
    phone         = Column(String(STR_MAX))
    name          = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    url           = Column(Unicode(STR_MAX))
    time          = Column(BigInteger)
    utcOffset     = Column(Integer)
    status        = Column(Unicode(STR_MAX))
    headcount     = Column(Integer)
    visibility    = Column(Unicode(STR_MAX))
    rsvpLimit     = Column(Integer)
    yesRsvpCount  = Column(Integer)
    maybeRsvpCount = Column(Integer)
    waitlistCount = Column(Integer)
    ratingCount   = Column(Integer)
    ratingAverage = Column(Float)
    feeRequired   = Column(Unicode(STR_MAX))
    feeCurrency   = Column(Unicode(STR_MAX))
    feeLabel      = Column(Unicode(STR_MAX))
    feeDescription = Column(Unicode(STR_MAX))
    feeAccepts    = Column(Unicode(STR_MAX))
    feeAmount     = Column(Float)
    createdDate   = Column(BigInteger)

class MUComment(SQLBase):
    __tablename__ = 'mucomment'
    id            = Column(BigInteger, primary_key=True)
    parentId      = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           nullable=False,
                           index=True)
    createdDate   = Column(BigInteger)
    inReplyTo     = Column(String(STR_MAX))
    description   = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))

class MULink(SQLBase):
    __tablename__ = 'mulink'
    id            = Column(BigInteger, primary_key=True)
    parentId      = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           nullable=False,
                           index=True)
    type          = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))

class GHProfile(SQLBase):
    __tablename__ = 'ghprofile'
    id                = Column(BigInteger, primary_key=True)
    profileId         = Column(String(STR_MAX), index=True, nullable=False)
    crawlNumber       = Column(BigInteger, index=True, nullable=False)
    name                   = Column(Unicode(STR_MAX))
    country                = Column(Unicode(STR_MAX))
    city                   = Column(Unicode(STR_MAX))
    company                = Column(Unicode(STR_MAX))
    createdDate            = Column(BigInteger)
    profileUrl             = Column(String(STR_MAX))
    profilePictureUrl      = Column(String(STR_MAX))
    login                  = Column(String(STR_MAX))
    email                  = Column(String(STR_MAX))
    contributionsCount     = Column(Integer)
    followersCount         = Column(Integer)
    followingCount         = Column(Integer)
    publicRepoCount        = Column(Integer)
    publicGistCount        = Column(Integer)
    indexedOn              = Column(BigInteger, index=True)
    crawledDate            = Column(BigInteger, index=True)
    crawlFailCount    = Column(BigInteger, index=True)
    
    links                  = relationship('GHLink',
                                          cascade='all, delete-orphan')
    repositories           = relationship('GHRepository',
                                          cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('profileId', 'crawlNumber'),)

class GHLink(SQLBase):
    __tablename__ = 'ghlink'
    id            = Column(BigInteger, primary_key=True)
    parentId      = Column(BigInteger,
                           ForeignKey('ghprofile.id'),
                           nullable=False,
                           index=True)
    type          = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))

class GHRepository(SQLBase):
    __tablename__ = 'ghrepository'
    id            = Column(BigInteger, primary_key=True)
    parentId      = Column(BigInteger,
                           ForeignKey('ghprofile.id'),
                           nullable=False,
                           index=True)    
    name            = Column(Unicode(STR_MAX))
    description     = Column(Unicode(STR_MAX))
    fullName        = Column(Unicode(STR_MAX))
    url             = Column(String(STR_MAX))
    gitUrl          = Column(String(STR_MAX))
    sshUrl          = Column(String(STR_MAX))
    createdDate     = Column(BigInteger)
    pushedDate      = Column(BigInteger)
    size            = Column(Integer)
    defaultBranch   = Column(String(STR_MAX))
    viewCount       = Column(Integer)
    subscribersCount = Column(Integer)
    forksCount      = Column(Integer)
    stargazersCount = Column(Integer)
    openIssuesCount = Column(Integer)
    tags            = Column(Array(Unicode(STR_MAX)))
    

class DatoinDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)
    
