__all__ = [
    'Webpage',
    'Link',
    'CrawlDB',
    ]

import conf
from dbtools import *
from sqlalchemy import \
    Column, \
    ForeignKey, \
    UniqueConstraint, \
    Index, \
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
from logger import Logger

STR_MAX = 100000

SQLBase = declarative_base()


class Webpage(SQLBase):
    __tablename__ = 'webpage'
    id            = Column(BigInteger, primary_key=True)
    site          = Column(String(20), index=True, nullable=False)
    url           = Column(String(STR_MAX), index=True, nullable=False)
    redirect_url  = Column(String(STR_MAX), index=True)
    timestamp     = Column(DateTime)
    html          = Column(Text)
    type          = Column(String(STR_MAX))
    valid         = Column(Boolean, nullable=False)
    fail_count    = Column(Integer, nullable=False)
    tag           = Column(String(STR_MAX))
    full_description = Column(Text)
    category      = Column(String(100))
    country       = Column(String(100))
    links         = relationship('Link',
                                 cascade='all, delete-orphan')

    __table_args__ = (
        UniqueConstraint('url', 'timestamp'),
        Index('ix_webpage_site_timestamp', 'site', 'timestamp'),
        Index('ix_webpage_site_type', 'site', 'type'),
        Index('ix_webpage_site_valid', 'site', 'valid'),
        Index('ix_webpage_site_fail_count', 'site', 'fail_count'),
        Index('ix_webpage_country_category', 'country', 'category'),
    )


class Link(SQLBase):
    __tablename__ = 'link'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('webpage.id'),
                           nullable=False,
                           index=True)
    url           = Column(String(STR_MAX), index=True, nullable=False)
    type          = Column(String(STR_MAX), index=True)
    tag           = Column(String(STR_MAX))

    __table_args__ = (UniqueConstraint('parent_id', 'url'),)


class CrawlDB(Session):
    def __init__(self, url=conf.CRAWL_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)

    def add_url(self, site, type, url, tag, category, country):
        q = self.query(Webpage.id) \
                .filter(Webpage.site == site,
                        Webpage.url == url)
        webpage = None
        if q.first() is None:
            webpage = Webpage(site=site, url=url,
                              type=type, tag=tag, fail_count=0, category=category, country=country, valid=False)
            self.add(webpage)
        return webpage
