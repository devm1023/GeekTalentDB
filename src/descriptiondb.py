__all__ = [
    'SectorDescription',
    'CareerDescription',
    'SkillDescription',
    'DescriptionDB',
    ]

from dbtools import *
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

import conf
from watsondb import *
import requests
from logger import Logger


STR_MAX = 100000

SQLBase = declarative_base()


class SectorDescription(SQLBase):
    __tablename__ = 'sector_description'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX), index=True)
    short_text    = Column(Unicode(STR_MAX))
    text          = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    source        = Column(Unicode(STR_MAX))
    approved      = Column(String(20))

    __table_args__ = (UniqueConstraint('name'),)


class CareerDescription(SQLBase):
    __tablename__ = 'career_description'
    id            = Column(BigInteger, primary_key=True)
    sector        = Column(Unicode(STR_MAX), index=True)
    name          = Column(Unicode(STR_MAX), index=True)
    short_text    = Column(Unicode(STR_MAX))
    text          = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    source        = Column(Unicode(STR_MAX))
    match_count   = Column(Integer)
    approved      = Column(String(20))

    __table_args__ = (UniqueConstraint('sector', 'name'),)


class SkillDescription(SQLBase):
    __tablename__ = 'skill_description'
    id            = Column(BigInteger, primary_key=True)
    sector        = Column(Unicode(STR_MAX), index=True)
    name          = Column(Unicode(STR_MAX), index=True)
    short_text    = Column(Unicode(STR_MAX))
    text          = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    source        = Column(Unicode(STR_MAX))
    match_count   = Column(Integer)
    approved      = Column(String(20))

    __table_args__ = (UniqueConstraint('sector', 'name'),)
    

class DescriptionDB(Session):
    def __init__(self, url=conf.DESCRIPTION_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)    

    def get_description(self, tpe, sector, name, watson_lookup=False,
                        logger=Logger(None)):
        if tpe == 'sector':
            table = SectorDescription
        elif tpe == 'career':
            table = CareerDescription
        elif tpe == 'skill':
            table = SkillDescription
        else:
            raise ValueError('Invalid entity type {0:s}'.repr(tpe))

        sectors = [sector]
        if sector is not None:
            sectors.append(None)

        entity = None
        for sector in sectors:
            if entity is not None:
                break
            if tpe != 'sector':
                entity = self.query(table) \
                             .filter(table.sector == sector,
                                     table.name == name) \
                             .first()
            else:
                entity = self.query(table) \
                             .filter(table.name == name) \
                             .first()
        if entity is not None:
            return dict_from_row(entity, pkeys=False)
        if not watson_lookup:
            return None

        logger.log('Looking up {0:s}...'.format(repr(name)))
        with WatsonDB() as wsdb:
            descriptions = wsdb.get_descriptions(name)
            if descriptions:
                r = descriptions[0]
                entity = table(name=name,
                               text=r.get('text', None),
                               url=r.get('url', None),
                               source='Wikipedia',
                               match_count=len(descriptions),
                               approved=None)
            else:
                entity = table(name=name,
                               match_count=0,
                               approved=None)
        logger.log('done.\n')

        self.add(entity)
        self.flush()
        return dict_from_row(entity, pkeys=False)

    def find_descriptions(self, tpe, queries, sector=None):
        if tpe == 'sector':
            table = SectorDescription
        elif tpe == 'career':
            table = CareerDescription
        elif tpe == 'skill':
            table = SkillDescription
        else:
            raise ValueError('Invalid entity type {0:s}'.repr(tpe))

        if not queries:
            return []
        
        q = self.query(table) \
                .filter(table.name.in_(queries))
        if sector is not None and tpe != 'sector':
            q = q.filter(table.sector == sector)
            
        return [dict_from_row(row, pkeys=False) for row in q]

