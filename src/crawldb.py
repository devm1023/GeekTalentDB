__all__ = [
    'Website',
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
    level         = Column(Integer, nullable=False)
    parent_url    = Column(String(STR_MAX), index=True)
    url           = Column(String(STR_MAX), index=True, nullable=False)
    redirect_url  = Column(String(STR_MAX), index=True)
    timestamp     = Column(DateTime, index=True)
    html          = Column(Text)
    valid         = Column(Boolean)
    leaf          = Column(Boolean)
    fail_count    = Column(Integer)

    __table_args__ = (UniqueConstraint('url', 'timestamp'),)


class CrawlDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def load_urls(self, site, leaf, filename,
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
                                     level=0, fail_count=0, valid=False))
                if count % batch_size == 0:
                    self.commit()
                    logger.log('{0:d} records processed.\n'.format(count))
            if count % batch_size != 0:
                logger.log('{0:d} records processed.\n'.format(count))
            self.commit()
