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
        if d is None:
            return None
        pkeycols, pkey = _getPkey(d, table)
        if None in pkey:
            raise ValueError('dict must contain all or no primary keys.')

        row = None
        if pkey is not None:
            whereclauses = [c == v for c, v in zip(pkeycols, pkey)]
            row = self.query(table).filter(*whereclauses).first()
        if row is None:
            row = rowFromDict(d, table)
            self.add(row)
        else:
            updateRowFromDict(row, d)

        return row


def _getPkey(d, table):
    pkeycols = inspect(table).primary_key
    pkeynames = [c.name for c in pkeycols]
    pkey = tuple(d.get(k, None) for k in pkeynames)
    
    if any(k is not None for k in pkey):
        return pkeycols, pkey
    else:
        return pkeycols, None
    

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
                pkeys = set()
                rows = []
                for v in val:
                    if v is None:
                        continue
                    pkeycols, pkey = _getPkey(v, rtype)
                    if pkey not in pkeys:
                        rows.append(rowFromDict(v, rtype))
                        if pkey is not None:
                            pkeys.add(pkey)
                setattr(result, r.key, rows)
            elif val is not None:
                setattr(result, r.key, rowFromDict(val, rtype))
            else:
                setattr(result, r.key, None)

    return result

def _mergeLists(rows, dicts, rowtype):
    if not rows:
        return [rowFromDict(d, rowtype) for d in dicts]
    mapper = inspect(rowtype)
    pkeynames = [c.name for c in mapper.primary_key]

    # find existing primary keys in dicts
    dict_pkeys = set()
    for d in dicts:
        ispresent = [d.get(k, None) is not None for k in pkeynames]
        haskeys = any(ispresent)
        if haskeys and not all(ispresent):
            raise ValueError('dict must contain all or no primary keys.')
        if haskeys:
            dict_pkeys.add(tuple(d[k] for k in pkeynames))

    # find rows with primary keys that are not in dicts
    sparerows = []
    for r in rows:
        pkeys = tuple(getattr(r, k) for k in pkeynames)
        if pkeys not in dict_pkeys:
            sparerows.append(r)
            
    # set primary keys in dicts that don't have one
    i = 0
    for d in dicts:
        if i >= len(sparerows):
            break
        if d.get(pkeynames[0], None) is None:
            for pkey in pkeynames:
                d[pkey] = getattr(sparerows[i], pkey)
            i += 1

    return [rowFromDict(d, rowtype) for d in dicts]

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
                val = [v for v in val if v is not None]
                setattr(row, r.key, _mergeLists(getattr(row, r.key), val, rtype))
            elif val is not None:
                setattr(row, r.key, rowFromDict(val, rtype))
            else:
                setattr(row, r.key, None)
    
    return row

