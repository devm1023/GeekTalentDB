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

    links         = relationship('Link',
                                 cascade='all, delete-orphan')

    __table_args__ = (
        UniqueConstraint('url', 'timestamp'),
        Index('ix_webpage_site_timestamp', 'site', 'timestamp'),
        Index('ix_webpage_site_type', 'site', 'type'),
        Index('ix_webpage_site_valid', 'site', 'valid'),
        Index('ix_webpage_site_fail_count', 'site', 'fail_count'),
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

    __table_args__ = (UniqueConstraint('parent_id', 'url'),)


class CrawlDB(Session):
    def __init__(self, url=conf.CRAWL_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)

    def add_url(self, site, type, url):
        q = self.query(Webpage.id) \
                .filter(Webpage.site == site,
                        Webpage.url == url)
        webpage = None
        if q.first() is None:
            webpage = Webpage(site=site, url=url,
                              type=type, fail_count=0, valid=False)
            self.add(webpage)
        return webpage
        
    def load_urls(self, site, type, filename,
                  batch_size=10000, logger=Logger(None)):
        with open(filename, 'r') as inputfile:
            count = 0
            for line in inputfile:
                count += 1
                url = line.strip()
                q = self.query(Webpage.id) \
                        .filter(Webpage.url == url)
                if q.first() is None:
                    self.add(Webpage(site=site, url=url,
                                     type=type, fail_count=0, valid=False))
                if count % batch_size == 0:
                    self.commit()
                    logger.log('{0:d} records processed.\n'.format(count))
            if count % batch_size != 0:
                logger.log('{0:d} records processed.\n'.format(count))
            self.commit()
