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
    Float, \
    func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ARRAY as Array


STR_MAX = 100000

SQLBase = declarative_base()


class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id            = Column(BigInteger, primary_key=True)
    profile_id    = Column(String(STR_MAX), index=True, nullable=False)
    crawl_number  = Column(BigInteger, index=True, nullable=False)
    name          = Column(Unicode(STR_MAX))
    last_name     = Column(Unicode(STR_MAX))
    first_name    = Column(Unicode(STR_MAX))
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    sector        = Column(Unicode(STR_MAX))
    title         = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    profile_url   = Column(String(STR_MAX))
    profile_picture_url = Column(String(STR_MAX))
    connections   = Column(String(STR_MAX))
    categories    = Column(Array(Unicode(STR_MAX)))
    indexed_on    = Column(BigInteger, index=True)
    crawled_date  = Column(BigInteger, index=True)
    crawl_fail_count = Column(BigInteger, index=True)

    experiences   = relationship('LIExperience',
                                 cascade='all, delete-orphan')
    educations    = relationship('LIEducation',
                                 cascade='all, delete-orphan')
    groups        = relationship('LIGroup',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('profile_id', 'crawl_number'),)

class LIExperience(SQLBase):
    __tablename__ = 'liexperience'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    date_from     = Column(BigInteger)
    date_to       = Column(BigInteger)
    description   = Column(Unicode(STR_MAX))

class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    degree        = Column(Unicode(STR_MAX))
    area          = Column(Unicode(STR_MAX))
    date_from     = Column(BigInteger)
    date_to       = Column(BigInteger)
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


class INProfile(SQLBase):
    __tablename__ = 'inprofile'
    id            = Column(BigInteger, primary_key=True)
    profile_id    = Column(String(STR_MAX), index=True, nullable=False)
    crawl_number  = Column(BigInteger, index=True, nullable=False)
    name          = Column(Unicode(STR_MAX))
    last_name     = Column(Unicode(STR_MAX))
    first_name    = Column(Unicode(STR_MAX))
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    title         = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    additional_information = Column(Unicode(STR_MAX))
    profile_url   = Column(String(STR_MAX))
    profile_updated_date = Column(BigInteger)
    indexed_on    = Column(BigInteger, index=True)
    crawled_date  = Column(BigInteger, index=True)
    crawl_fail_count = Column(BigInteger, index=True)

    experiences   = relationship('INExperience',
                                 cascade='all, delete-orphan')
    educations    = relationship('INEducation',
                                 cascade='all, delete-orphan')
    certifications = relationship('INCertification',
                                  cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('profile_id', 'crawl_number'),)

class INExperience(SQLBase):
    __tablename__ = 'inexperience'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('inprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    date_from     = Column(BigInteger)
    date_to       = Column(BigInteger)
    description   = Column(Unicode(STR_MAX))

class INEducation(SQLBase):
    __tablename__ = 'ineducation'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('inprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    degree        = Column(Unicode(STR_MAX))
    area          = Column(Unicode(STR_MAX))
    date_from     = Column(BigInteger)
    date_to       = Column(BigInteger)
    description   = Column(Unicode(STR_MAX))

class INCertification(SQLBase):
    __tablename__ = 'incertification'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('inprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    date_from     = Column(BigInteger)
    date_to       = Column(BigInteger)
    description   = Column(Unicode(STR_MAX))


class UWProfile(SQLBase):
    __tablename__ = 'uwprofile'
    id            = Column(BigInteger, primary_key=True)
    profile_id    = Column(String(STR_MAX), index=True, nullable=False)
    crawl_number  = Column(BigInteger, index=True, nullable=False)
    name          = Column(Unicode(STR_MAX))
    last_name     = Column(Unicode(STR_MAX))
    first_name    = Column(Unicode(STR_MAX))
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    title         = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    profile_url   = Column(String(STR_MAX))
    profile_picture_url = Column(String(STR_MAX))
    categories    = Column(Array(Unicode(STR_MAX)))
    indexed_on    = Column(BigInteger, index=True)
    crawled_date  = Column(BigInteger, index=True)
    crawl_fail_count  = Column(BigInteger, index=True)

    experiences   = relationship('UWExperience',
                                 cascade='all, delete-orphan')
    educations    = relationship('UWEducation',
                                 cascade='all, delete-orphan')
    tests         = relationship('UWTest',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('profile_id', 'crawl_number'),)

class UWExperience(SQLBase):
    __tablename__ = 'uwexperience'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('uwprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    date_from     = Column(BigInteger)
    date_to       = Column(BigInteger)
    description   = Column(Unicode(STR_MAX))

class UWEducation(SQLBase):
    __tablename__ = 'uweducation'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('uwprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    degree        = Column(Unicode(STR_MAX))
    area          = Column(Unicode(STR_MAX))
    date_from     = Column(BigInteger)
    date_to       = Column(BigInteger)
    description   = Column(Unicode(STR_MAX))

class UWTest(SQLBase):
    __tablename__ = 'uwtest'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('uwprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    score         = Column(Float)
    test_percentile = Column(Float)
    test_date     = Column(BigInteger)
    test_duration = Column(Float)

class MUProfile(SQLBase):
    __tablename__ = 'muprofile'
    id            = Column(BigInteger, primary_key=True)
    profile_id    = Column(String(STR_MAX), index=True, nullable=False)
    crawl_number  = Column(BigInteger, index=True, nullable=False)
    name          = Column(Unicode(STR_MAX))
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    latitude      = Column(Float)
    longitude     = Column(Float)
    status        = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    profile_url   = Column(String(STR_MAX))
    profile_picture_id = Column(String(STR_MAX))
    profile_picture_url = Column(String(STR_MAX))
    profile_hq_picture_url = Column(String(STR_MAX))
    profile_thumb_picture_url = Column(String(STR_MAX))
    categories    = Column(Array(Unicode(STR_MAX)))
    indexed_on    = Column(BigInteger, index=True)
    crawled_date  = Column(BigInteger, index=True)
    crawl_fail_count = Column(BigInteger, index=True)

    groups        = relationship('MUGroup',
                                 cascade='all, delete-orphan')
    events        = relationship('MUEvent',
                                 cascade='all, delete-orphan')
    comments      = relationship('MUComment',
                                 cascade='all, delete-orphan')
    links         = relationship('MULink',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('profile_id', 'crawl_number'),)

class MUGroup(SQLBase):
    __tablename__ = 'mugroup'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           nullable=False,
                           index=True)
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    latitude      = Column(Float)
    longitude     = Column(Float)
    timezone      = Column(Unicode(STR_MAX))
    utc_offset    = Column(Integer)
    name          = Column(Unicode(STR_MAX))
    category_name = Column(Unicode(STR_MAX))
    category_shortname = Column(Unicode(STR_MAX))
    category_id   = Column(String(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    urlname       = Column(String(STR_MAX))
    picture_url   = Column(String(STR_MAX))
    picture_id    = Column(BigInteger)
    hq_picture_url = Column(String(STR_MAX))
    thumb_picture_url = Column(String(STR_MAX))
    join_mode     = Column(Unicode(STR_MAX))
    rating        = Column(Float)
    organizer_name = Column(Unicode(STR_MAX))
    organizer_id  = Column(String(STR_MAX))
    members       = Column(Integer)
    state         = Column(Unicode(STR_MAX))
    visibility    = Column(Unicode(STR_MAX))
    who           = Column(Unicode(STR_MAX))
    categories    = Column(Array(Unicode(STR_MAX)))
    created_date  = Column(BigInteger)

class MUEvent(SQLBase):
    __tablename__ = 'muevent'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           nullable=False,
                           index=True)
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    address_line1 = Column(Unicode(STR_MAX))
    address_line2 = Column(Unicode(STR_MAX))
    latitude      = Column(Float)
    longitude     = Column(Float)
    phone         = Column(String(STR_MAX))
    name          = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    url           = Column(Unicode(STR_MAX))
    time          = Column(BigInteger)
    utc_offset    = Column(Integer)
    status        = Column(Unicode(STR_MAX))
    headcount     = Column(Integer)
    visibility    = Column(Unicode(STR_MAX))
    rsvp_limit    = Column(Integer)
    yes_rsvp_count = Column(Integer)
    maybe_rsvp_count = Column(Integer)
    waitlist_count = Column(Integer)
    rating_count  = Column(Integer)
    rating_average = Column(Float)
    fee_required  = Column(Unicode(STR_MAX))
    fee_currency  = Column(Unicode(STR_MAX))
    fee_label     = Column(Unicode(STR_MAX))
    fee_description = Column(Unicode(STR_MAX))
    fee_accepts   = Column(Unicode(STR_MAX))
    fee_amount    = Column(Float)
    created_date  = Column(BigInteger)

class MUComment(SQLBase):
    __tablename__ = 'mucomment'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           nullable=False,
                           index=True)
    created_date  = Column(BigInteger)
    in_reply_to   = Column(String(STR_MAX))
    description   = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))

class MULink(SQLBase):
    __tablename__ = 'mulink'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           nullable=False,
                           index=True)
    type          = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))

class GHProfile(SQLBase):
    __tablename__ = 'ghprofile'
    id            = Column(BigInteger, primary_key=True)
    profile_id    = Column(String(STR_MAX), index=True, nullable=False)
    crawl_number  = Column(BigInteger, index=True, nullable=False)
    name          = Column(Unicode(STR_MAX))
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    created_date  = Column(BigInteger)
    profile_url   = Column(String(STR_MAX))
    profile_picture_url = Column(String(STR_MAX))
    login         = Column(String(STR_MAX))
    email         = Column(String(STR_MAX))
    contributions_count = Column(Integer)
    followers_count = Column(Integer)
    following_count = Column(Integer)
    public_repo_count = Column(Integer)
    public_gist_count = Column(Integer)
    indexed_on    = Column(BigInteger, index=True)
    crawled_date  = Column(BigInteger, index=True)
    crawl_fail_count = Column(BigInteger, index=True)

    links         = relationship('GHLink',
                                 cascade='all, delete-orphan')
    repositories  = relationship('GHRepository',
                                          cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('profile_id', 'crawl_number'),)

class GHLink(SQLBase):
    __tablename__ = 'ghlink'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('ghprofile.id'),
                           nullable=False,
                           index=True)
    type          = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))

class GHRepository(SQLBase):
    __tablename__ = 'ghrepository'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('ghprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    full_name     = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    git_url       = Column(String(STR_MAX))
    ssh_url       = Column(String(STR_MAX))
    created_date  = Column(BigInteger)
    pushed_date   = Column(BigInteger)
    size          = Column(Integer)
    default_branch = Column(String(STR_MAX))
    view_count    = Column(Integer)
    subscribers_count = Column(Integer)
    forks_count   = Column(Integer)
    stargazers_count = Column(Integer)
    open_issues_count = Column(Integer)
    tags          = Column(Array(Unicode(STR_MAX)))


class DatoinDB(Session):
    def __init__(self, url=conf.DATOIN_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)
    
