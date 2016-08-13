__all__ = [
    'Website',
    'Link',
    'CrawlDB',
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
from logger import Logger

STR_MAX = 100000

SQLBase = declarative_base()


class Website(SQLBase):
    __tablename__ = 'website'
    id            = Column(BigInteger, primary_key=True)
    site          = Column(String(20), index=True, nullable=False)
    url           = Column(String(STR_MAX), index=True, nullable=False)
    redirect_url  = Column(String(STR_MAX), index=True)
    timestamp     = Column(DateTime, index=True)
    html          = Column(Text)
    level         = Column(Integer, index=True)
    valid         = Column(Boolean, index=True)
    fail_count    = Column(Integer, index=True)

    links         = relationship('Link',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('url', 'timestamp'),)


class Link(SQLBase):
    __tablename__ = 'link'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('website.id'),
                           nullable=False,
                           index=True)
    url           = Column(String(STR_MAX), index=True, nullable=False)
    level         = Column(Integer, index=True)

    __table_args__ = (UniqueConstraint('parent_id', 'url'),)


class CrawlDB(Session):
    def __init__(self, url=conf.CRAWL_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)
    
    def load_urls(self, site, level, filename,
                  batch_size=10000, logger=Logger(None)):
        with open(filename, 'r') as inputfile:
            count = 0
            for line in inputfile:
                count += 1
                url = line.strip()
                q = self.query(Website.id) \
                        .filter(Website.url == url)
                if q.first() is None:
                    self.add(Website(site=site, url=url,
                                     level=level, fail_count=0, valid=False))
                if count % batch_size == 0:
                    self.commit()
                    logger.log('{0:d} records processed.\n'.format(count))
            if count % batch_size != 0:
                logger.log('{0:d} records processed.\n'.format(count))
            self.commit()
