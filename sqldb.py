__all__ = ['SQLDatabase', 'sqlbase']

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base as sqlbase
from sqlalchemy import create_engine

class SQLDatabase:
    def __init__(self, metadata, url=None, session=None, engine=None):
        if session is None and engine is None and url is None:
            raise ValueError('One of url, session, or engine must be specified')
        if session is None:
            if engine is None:
                engine = create_engine(url)
            session = sessionmaker(bind=engine)()
        self.metadata = metadata
        self.session = session
        self.query = session.query
        self.flush = session.flush
        self.commit = session.commit
        self.add = session.add

    def drop_all(self):
        self.metadata.drop_all(self.session.bind)

    def create_all(self):
        self.metadata.create_all(self.session.bind)
