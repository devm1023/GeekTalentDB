__all__ = [
    'SQLDatabase',
    'sqlbase',
    'dictFromRow',
    'rowFromDict',
    'updateRowFromDict',
]

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.collections import bulk_replace
from sqlalchemy.ext.declarative import declarative_base as sqlbase
from sqlalchemy import create_engine, inspect
from copy import deepcopy
from pprint import pprint

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
        self.execute = session.execute

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.session.close()
        return False

    def drop_all(self):
        self.metadata.drop_all(self.session.bind)

    def create_all(self):
        self.metadata.create_all(self.session.bind)

    def addFromDict(self, d, table, update=True, flush=False):
        if d is None:
            return None
        pkeycols, pkey = _getPkey(d, table)
        if pkey is not None and None in pkey:
            raise ValueError('dict must contain all or no primary keys.')

        if not update:
            row = rowFromDict(d, table)
            self.add(row)
        else:
            row = None
            if pkey is not None:
                whereclauses = [c == v for c, v in zip(pkeycols, pkey)]
                row = self.query(table).filter(*whereclauses).first()
            if row is None:
                row = rowFromDict(d, table)
                self.add(row)
            else:
                updateRowFromDict(row, d)
                
        if flush:
            self.flush()
            _updateIds(d, row)
        return row

def _updateIds(d, row, fkeynames=[]):
    mapper = inspect(type(row))
    pkeycols = mapper.primary_key
    for c in pkeycols:
        d[c.key] = getattr(row, c.key)
    for k in fkeynames:
        d[k] = getattr(row, k)
    for relation in mapper.relationships:
        if relation.key not in d:
            continue
        fkeynames = []
        for l, r in relation.local_remote_pairs:
            fkeynames.append(r.key)
            d[l.key] = getattr(row, l.key)
        subdicts = d[relation.key]
        subrows = getattr(row, relation.key)
        if isinstance(subdicts, list):
            subdicts = [s for s in subdicts if s is not None]
            for subdict, subrow in zip(subdicts, subrows):
                _updateIds(subdict, subrow, fkeynames)
        else:
            _updateIds(subdicts, subrows, fkeynames)
    
def _getPkey(d, table):
    pkeycols = inspect(table).primary_key
    pkeynames = [c.key for c in pkeycols]
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
        setattr(result, c.key, d.get(c.key, None))
            
    for relation in mapper.relationships:
        if relation.key in d:
            val = d[relation.key]
            remotetype = relation.mapper.class_
            lrpairs = [(l.key, r.key) for l, r in relation.local_remote_pairs]
            if isinstance(val, list):
                rows = []
                for v in val:
                    if v is None:
                        continue
                    for l, r in lrpairs:
                        v[r] = d.get(l, None)
                    rows.append(rowFromDict(v, remotetype))
                setattr(result, relation.key, rows)
            elif val is not None:
                setattr(result, relation.key, rowFromDict(val, remotetype))
            else:
                setattr(result, relation.key, None)

    return result

def _mergeLists(rows, dicts, rowtype):
    if not rows:
        return [rowFromDict(d, rowtype) for d in dicts]
    mapper = inspect(rowtype)
    nipkeynames = [c.key for c in mapper.primary_key if not c.autoincrement]
    aipkeynames = [c.key for c in mapper.primary_key if c.autoincrement]

    keymap = {}
    for row in rows:
        nipkey = tuple(getattr(row, k) for k in nipkeynames)
        if all(k is not None for k in nipkey):
            if nipkey in keymap:
                keymap[nipkey].insert(0, row)
            else:
                keymap[nipkey] = [row]
            
    newrows = []
    for d in dicts:
        if d is None:
            continue
        nipkey = tuple(d.get(k, None) for k in nipkeynames)
        if nipkey in keymap:
            row = keymap[nipkey].pop(0)
            for aipkey in aipkeynames:
                if d.get(aipkey, None) is None:
                    d[aipkey] = getattr(row, aipkey)
            newrows.append(updateRowFromDict(row, d))
            if not keymap[nipkey]:
                del keymap[nipkey]
        else:
            newrows.append(rowFromDict(d, rowtype))

    return newrows

def updateRowFromDict(row, d):
    mapper = inspect(type(row))
    
    for c in mapper.column_attrs:
        if c.key in d:
            setattr(row, c.key, d[c.key])

    for relation in mapper.relationships:
        if relation.key in d:
            val = d[relation.key]
            remotetype = relation.mapper.class_
            lrpairs = [(l.key, r.key) for l, r in relation.local_remote_pairs]
            if isinstance(val, list):
                val = [v for v in val if v is not None]
                for v in val:
                    for l, r in lrpairs:
                        v[r] = d.get(l, None)
                collection = getattr(row, relation.key)
                newcollection = _mergeLists(collection, val, remotetype)
                setattr(row, relation.key, newcollection)
            elif val is not None:
                setattr(row, relation.key, rowFromDict(val, remotetype))
            else:
                setattr(row, relation.key, None)

    return row

