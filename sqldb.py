__all__ = ['SQLBase', 'SQLDatabase']

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

SQLBase = declarative_base()

class SQLDatabase:
    def __init__(self, url=None, session=None, engine=None):
        if session is None and engine is None and url is None:
            raise ValueError('One of url, session, or engine must be specified')
        if session is None:
            if engine is None:
                engine = create_engine(url)
            session = sessionmaker(bind=engine)()
        self.session = session
        self.query = session.query
        self.flush = session.flush
        self.commit = session.commit
        self.add = session.add

    def drop_all(self):
        SQLBase.metadata.drop_all(self.session.bind)

    def create_all(self):
        SQLBase.metadata.create_all(self.session.bind)
