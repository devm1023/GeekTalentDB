__all__ = [
    'SQLDatabase',
    'sqlbase',
    'dict_from_row',
    'row_from_dict',
    'update_row_from_dict',
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
            self.Session = sessionmaker(bind=engine)
        else:
            self.Session = session

        self.session = None
        self.metadata = metadata
        self.new_session()

    def new_session(self):
        if self.session is not None:
            self.session.close()
        self.session = self.Session()
        self.query = self.session.query
        self.flush = self.session.flush
        self.commit = self.session.commit
        self.add = self.session.add
        self.execute = self.session.execute
        self.close = self.session.close
        
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.session.close()
        return False

    def query(*args, **kwargs):
        return self.session.query(*args, **kwargs)

    
    
    def drop_all(self):
        self.metadata.drop_all(self.session.bind)

    def create_all(self):
        self.metadata.create_all(self.session.bind)

    def add_from_dict(self, d, table, update=True, flush=False, delete=True,
                      protect=[]):
        if d is None:
            return None
        pkeycols, pkey = _get_pkey(d, table)
        if pkey is not None and None in pkey:
            raise ValueError('dict must contain all or no primary keys.')

        if not update:
            row = row_from_dict(d, table)
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
                row = row_from_dict(d, table)
                self.add(row)
            else:
                update_row_from_dict(row, d, protect=protect, delete=delete)

        if flush:
            self.flush()
            _update_ids(d, row)
        return row

def _update_ids(d, row, fkeynames=[]):
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
                _update_ids(subdict, subrow, fkeynames)
        else:
            _update_ids(subdicts, subrows, fkeynames)

def _get_pkey(d, table):
    pkeycols = inspect(table).primary_key
    pkeynames = [c.key for c in pkeycols]
    pkey = tuple(d.get(k, None) for k in pkeynames)

    if any(k is not None for k in pkey):
        return pkeycols, pkey
    else:
        return pkeycols, None

def dict_from_row(row, pkeys=True, fkeys=True, exclude=[]):
    if row is None:
        return None
    if isinstance(row, list):
        return [dict_from_row(r, pkeys=pkeys, fkeys=fkeys, exclude=exclude) \
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
        result[relation.key] = dict_from_row(getattr(row, relation.key),
                                           pkeys=pkeys, fkeys=fkeys,
                                           exclude=fkeynames)

    return result

def row_from_dict(d, rowtype):
    result = rowtype()
    mapper = inspect(rowtype)

    for c in mapper.column_attrs:
        setattr(result, c.key, d.get(c.key, None))

    for relation in mapper.relationships:
        is_one_to_many = relation.direction.name == 'ONETOMANY'
        if relation.key in d:
            val = d[relation.key]
            remotetype = relation.mapper.class_
            lrpairs = [(l.key, r.key) for l, r in relation.local_remote_pairs]
            if is_one_to_many:
                rows = []
                for v in val:
                    if v is None:
                        continue
                    for l, r in lrpairs:
                        v[r] = d.get(l, None)
                    rows.append(row_from_dict(v, remotetype))
                setattr(result, relation.key, rows)
            elif val is not None:
                setattr(result, relation.key, row_from_dict(val, remotetype))
            else:
                setattr(result, relation.key, None)

    return result

def _merge_lists(rows, dicts, rowtype, strict=False, delete=True, protect=[]):
    if not rows:
        return [row_from_dict(d, rowtype) for d in dicts]
    mapper = inspect(rowtype)
    pkeynames = [c.key for c in mapper.primary_key]
    ucs = []
    for constraint in rowtype.__table__.constraints:
        if not isinstance(constraint, UniqueConstraint):
            continue
        if any(c.nullable for c in constraint.columns):
            continue
        ucs.append(tuple(c.key for c in constraint.columns))


    keymap = {}
    ucmaps = [{} for uccols in ucs]
    unmatched = []
    for row in rows:
        pkey = tuple(getattr(row, k) for k in pkeynames)
        keymap[pkey] = row
        for uccols, ucmap in zip(ucs, ucmaps):
            ucvals = tuple(getattr(row, c) for c in uccols)
            ucmap[ucvals] = pkey

    newrows = []
    unmatcheddicts = []
    for d in dicts:
        if d is None:
            continue
        pkey = tuple(d.get(k, None) for k in pkeynames)
        if any(k is None for k in pkey) and any(k is not None for k in pkey):
            raise ValueError('Primary key must be fully specified or fully '
                             'unspecified.')
        if all(k is None for k in pkey):
            for uccols, ucmap in zip(ucs, ucmaps):
                ucvals = tuple(d.get(c, None) for c in uccols)
                if ucvals in ucmap:
                    pkey = ucmap.pop(ucvals)
                    break
        if pkey in keymap:
            row = keymap.pop(pkey)
            newrows.append(update_row_from_dict(row, d, strict=strict,
                                             protect=protect))
        else:
            unmatcheddicts.append(d)

    if delete:
        keymap = list(keymap.items())
    else:
        newrows.extend(keymap.values())
        keymap = []
    for d in unmatcheddicts:
        if keymap:
            pkey, row = keymap.pop()
            for name, val in zip(pkeynames, pkey):
                d[name] = val
        else:
            row = rowtype()
        newrows.append(update_row_from_dict(row, d, strict=True))

    return newrows


def update_row_from_dict(row, d, delete=True, protect=[], strict=False):
    mapper = inspect(type(row))
    protected_fields = set()
    protected_relations = {}
    if not strict:
        for field in protect:
            if isinstance(field, str):
                protected_fields.add(field)
            elif len(field) == 1:
                protected_fields.add(field[0])
            else:
                relation = field[0]
                if relation in protected_relations:
                    protected_relations[relation].append(field[1:])
                else:
                    protected_relations[relation] = [field[1:]]
    if strict:
        delete = True

    for c in mapper.primary_key:
        d[c.key] = getattr(row, c.key)

    for c in mapper.column_attrs:
        if c.key in protected_fields:
            continue
        if c.key in d:
            setattr(row, c.key, d[c.key])
        elif strict:
            setattr(row, c.key, None)

    for relation in mapper.relationships:
        if relation.key in protected_fields:
            continue
        is_one_to_many = relation.direction.name == 'ONETOMANY'
        if relation.key in d:
            val = d[relation.key]
            remotetype = relation.mapper.class_
            lrpairs = [(l.key, r.key) for l, r in relation.local_remote_pairs]
            if is_one_to_many:
                val = [v for v in val if v is not None]
                for v in val:
                    for l, r in lrpairs:
                        v[r] = d.get(l, None)
                collection = getattr(row, relation.key)
                newcollection = _merge_lists(
                    collection, val, remotetype, strict=strict, delete=delete,
                    protect=protected_relations.get(relation.key, []))
                setattr(row, relation.key, newcollection)
            elif val is not None:
                setattr(row, relation.key, row_from_dict(val, remotetype))
            else:
                setattr(row, relation.key, None)
        elif strict:
            if is_one_to_many:
                setattr(row, relation.key, [])
            else:
                setattr(row, relation.key, None)

    return row

