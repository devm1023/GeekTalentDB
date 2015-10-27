__all__ = [
    'SQLDatabase',
    'sqlbase',
    'dictFromRow',
    'rowFromDict',
    'updateRowFromDict',
]

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base as sqlbase
from sqlalchemy import create_engine, inspect

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

    def addFromDict(self, d, table):
        pkeycols = inspect(table).primary_key
        pkeynames = [c.name for c in pkeycols]

        pkeyset = set(pkeynames)
        dictkeyset = set([k for k, v in d.items() if v is not None])
        has_pkeys = pkeyset.issubset(dictkeyset)
        if not has_pkeys and pkeyset.intersection(dictkeyset):
            raise ValueError('dict must contain all or no primary keys.')

        row = None
        if has_pkeys:
            whereclauses = [c == d[n] for c, n in zip(pkeycols, pkeynames)]
            row = self.query(table).filter(*whereclauses).first()
        if row is None:
            row = rowFromDict(d, table)
            self.add(row)
        else:
            updateRowFromDict(row, d)

        return row


def dictFromRow(row):
    if row is None:
        return None
    if isinstance(row, list):
        return [dictFromRow(r) for r in row]
    
    mapper = inspect(type(row))
    result = {}

    for c in mapper.column_attrs:
        result[c.key] = getattr(row, c.key)

    for r in mapper.relationships:
        result[r.key] = dictFromRow(getattr(row, r.key))

    return result

def rowFromDict(d, rowtype):
    result = rowtype()
    mapper = inspect(rowtype)
    
    for c in mapper.column_attrs:
        if c.key in d:
            setattr(result, c.key, d[c.key])
            
    for r in mapper.relationships:
        if r.key in d:
            val = d[r.key]
            rtype = r.mapper.class_
            if isinstance(val, list):
                setattr(result, r.key, [rowFromDict(i, rtype) for i in val])
            elif val is not None:
                setattr(result, r.key, rowFromDict(val, rtype))
            else:
                setattr(result, r.key, None)

    return result

def updateRowFromDict(row, d):
    mapper = inspect(type(row))
    
    for c in mapper.column_attrs:
        if c.key in d:
            setattr(row, c.key, d[c.key])

    for r in mapper.relationships:
        if r.key in d:
            val = d[r.key]
            rtype = r.mapper.class_
            if isinstance(val, list):
                setattr(row, r.key, [rowFromDict(i, rtype) for i in val])
            elif val is not None:
                setattr(row, r.key, rowFromDict(val, rtype))
            else:
                setattr(row, r.key, None)
    
    return row

