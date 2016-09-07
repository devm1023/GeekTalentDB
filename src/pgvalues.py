from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FromClause
from sqlalchemy import select
from sqlalchemy.sql import column

class values(FromClause):
    named_with_column = True

    def __init__(self, columns, *args, **kw):
        self._column_args = columns
        self.list = args
        self.alias_name = self.name = kw.pop('alias_name', None)

    def _populate_column_collection(self):
        for c in self._column_args:
            c._make_proxy(self)


@compiles(values)
def compile_values(element, compiler, asfrom=False, **kw):
    columns = element.columns
    v = "VALUES %s" % ", ".join(
        "(%s)" % ", ".join(
                compiler.render_literal_value(elem, column.type)
                for elem, column in zip(tup, columns))
        for tup in element.list
    )
    v = v.replace('%', '%%')
    if asfrom:
        if element.alias_name:
            v = "(%s) AS %s (%s)" % (v, element.alias_name, (", ".join(c.name for c in element.columns)))
        else:
            v = "(%s)" % v
    return v


def in_values(col, vals, alias_name='_values_', column_name='_column_'):
    t = values([column(column_name, col.type)],
               *[(val,) for val in vals if val is not None],
               alias_name=alias_name)
    subq = select([getattr(t.c, column_name)]).alias()
    if None in vals:
        return (col == None) | col.in_(subq)
    else:
        return col.in_(subq)


if __name__ == '__main__':
    from sqlalchemy import MetaData, create_engine, String, Integer, Table, Column
    from sqlalchemy.orm import Session, mapper
    m1 = MetaData()
    class T(object):
        pass
    t1 = Table('mytable', m1, Column('mykey', Integer, primary_key=True),
                    Column('mytext', String),
                    Column('myint', Integer))
    mapper(T, t1)
    e = create_engine("postgresql://geektalent:geektalent@localhost/scratch", echo=True)
    m1.create_all(e)
    sess = Session(e)
    q = sess.query(T).filter(in_values(T.mykey, [1, 2]))
    for row in q:
        pass

