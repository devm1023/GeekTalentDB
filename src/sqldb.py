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
from sqlalchemy import create_engine, inspect, UniqueConstraint
from copy import deepcopy

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
            else:
                for constraint in table.__table__.constraints:
                    if not isinstance(constraint, UniqueConstraint):
                        continue
                    if any(c.nullable for c in constraint.columns):
                        continue
                    if any(d.get(c.key, None) is None \
                           for c in constraint.columns):
                        continue
                    whereclauses = [c == d[c.key] for c in constraint.columns]
                    row = self.query(table).filter(*whereclauses).first()
                    if row is not None:
                        break
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

def dictFromRow(row, pkeys=True, fkeys=True, exclude=[]):
    if row is None:
        return None
    if isinstance(row, list):
        return [dictFromRow(r, pkeys=pkeys, fkeys=fkeys, exclude=exclude) \
                for r in row]
    
    mapper = inspect(type(row))
    result = {}

    pkeynames = [c.key for c in mapper.primary_key]
    
    for c in mapper.column_attrs:
        if not pkeys and c.key in pkeynames:
            continue
        if c.key in exclude:
            continue
        result[c.key] = getattr(row, c.key)

    for relation in mapper.relationships:
        if relation.key in exclude:
            continue
        fkeynames = []
        if not fkeys:
            fkeynames = [r.key for l, r in relation.local_remote_pairs]
        result[relation.key] = dictFromRow(getattr(row, relation.key),
                                           pkeys=pkeys, fkeys=fkeys,
                                           exclude=fkeynames)

    return result

def rowFromDict(d, rowtype):
    result = rowtype()
    mapper = inspect(rowtype)
    
    for c in mapper.column_attrs:
        setattr(result, c.key, d.get(c.key, None))
            
    for relation in mapper.relationships:
        isOneToMany = relation.direction.name == 'ONETOMANY'
        if relation.key in d:
            val = d[relation.key]
            remotetype = relation.mapper.class_
            lrpairs = [(l.key, r.key) for l, r in relation.local_remote_pairs]
            if isOneToMany:
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

def _mergeLists(rows, dicts, rowtype, strict=False):
    if not rows:
        return [rowFromDict(d, rowtype) for d in dicts]
    mapper = inspect(rowtype)
    pkeynames = [c.key for c in mapper.primary_key]

    keymap = {}
    unmatched = []
    for row in rows:
        pkey = tuple(getattr(row, k) for k in pkeynames)
        keymap[pkey] = row

    newrows = []
    unmatcheddicts = []
    for d in dicts:
        if d is None:
            continue
        pkey = tuple(d.get(k, None) for k in pkeynames)
        if any(k is None for k in pkey) and any(k is not None for k in pkey):
            raise ValueError('Primary key must be fully specified or fully '
                             'unspecified.')
        if pkey in keymap:
            row = keymap.pop(pkey)
            newrows.append(updateRowFromDict(row, d, strict=strict))
        else:
            unmatcheddicts.append(d)

    keymap = list(keymap.items())
    for d in unmatcheddicts:
        if keymap:
            pkey, row = keymap.pop()
            for name, val in zip(pkeynames, pkey):
                d[name] = val
        else:
            row = rowtype()
        newrows.append(updateRowFromDict(row, d, strict=True))
    
    return newrows


def updateRowFromDict(row, d, strict=False):
    mapper = inspect(type(row))

    for c in mapper.primary_key:
        d[c.key] = getattr(row, c.key)
    
    for c in mapper.column_attrs:
        if c.key in d:
            setattr(row, c.key, d[c.key])
        elif strict:
            setattr(row, c.key, None)

    for relation in mapper.relationships:
        isOneToMany = relation.direction.name == 'ONETOMANY'
        if relation.key in d:
            val = d[relation.key]
            remotetype = relation.mapper.class_
            lrpairs = [(l.key, r.key) for l, r in relation.local_remote_pairs]
            if isOneToMany:
                val = [v for v in val if v is not None]
                for v in val:
                    for l, r in lrpairs:
                        v[r] = d.get(l, None)
                collection = getattr(row, relation.key)
                newcollection = _mergeLists(collection, val, remotetype,
                                            strict=strict)
                setattr(row, relation.key, newcollection)
            elif val is not None:
                setattr(row, relation.key, rowFromDict(val, remotetype))
            else:
                setattr(row, relation.key, None)
        elif strict:
            setattr(row, relation.key, None)

    return row

