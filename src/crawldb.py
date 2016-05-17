__all__ = [
    'LIProfile',
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

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id            = Column(BigInteger, primary_key=True)
    url           = Column(String(STR_MAX), index=True, nullable=False)    
    redirect_url  = Column(String(STR_MAX), index=True)
    timestamp     = Column(DateTime, index=True)
    body          = Column(Text)
    valid         = Column(Boolean)
    fail_count    = Column(Integer)



class CrawlDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def load_urls(self, filename, batch_size=10000, logger=Logger(None)):
        with open(filename, 'r') as inputfile:
            count = 0
            for line in inputfile:
                count += 1
                url = line.strip()
                q = self.query(LIProfile.id) \
                        .filter(LIProfile.url == url)
                if q.first() is None:
                    self.add(LIProfile(url=url, fail_count=0, valid=False))
                if count % batch_size == 0:
                    self.commit()
                    logger.log('{0:d} records processed.\n'.format(count))
            if count % batch_size != 0:
                logger.log('{0:d} records processed.\n'.format(count))
            self.commit()

