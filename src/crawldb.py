__all__ = [
    'Website',
    'Link',
    'CrawlDB',
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
    DateTime, \
    Float, \
    Boolean, \
    func
from sqlalchemy.orm import relationship
from logger import Logger

STR_MAX = 100000

SQLBase = sqlbase()


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
    leaf          = Column(Boolean, index=True)
    fail_count    = Column(Integer, index=True)

    __table_args__ = (UniqueConstraint('url', 'timestamp'),)


class Link(SQLBase):
    __tablename__ = 'link'
    id            = Column(BigInteger, primary_key=True)
    site          = Column(String(20), index=True, nullable=False)
    from_url      = Column(String(STR_MAX), index=True, nullable=False)
    to_url        = Column(String(STR_MAX), index=True, nullable=False)
    timestamp     = Column(DateTime, index=True, nullable=False)

    __table_args__ = (UniqueConstraint('from_url', 'to_url', 'timestamp'),)
    

class CrawlDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def load_urls(self, site, leaf, level, filename,
                  batch_size=10000, logger=Logger(None)):
        with open(filename, 'r') as inputfile:
            count = 0
            for line in inputfile:
                count += 1
                url = line.strip()
                q = self.query(Website.id) \
                        .filter(Website.url == url)
                if q.first() is None:
                    self.add(Website(site=site, url=url, leaf=leaf,
                                     level=level, fail_count=0, valid=False))
                if count % batch_size == 0:
                    self.commit()
                    logger.log('{0:d} records processed.\n'.format(count))
            if count % batch_size != 0:
                logger.log('{0:d} records processed.\n'.format(count))
            self.commit()
