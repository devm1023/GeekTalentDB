__all__ = [
    'CareerDescription',
    'SkillDescription',
    'DescriptionDB',
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


STR_MAX = 100000

SQLBase = sqlbase()


class CareerDescription(SQLBase):
    __tablename__ = 'career_description'
    id            = Column(BigInteger, primary_key=True)
    sector        = Column(Unicode(STR_MAX), index=True)
    name          = Column(Unicode(STR_MAX), index=True)
    short_description = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    source        = Column(Unicode(STR_MAX))
    match_count   = Column(Integer)
    approved      = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('sector', 'name'),)


class SkillDescription(SQLBase):
    __tablename__ = 'skill_description'
    id            = Column(BigInteger, primary_key=True)
    sector        = Column(Unicode(STR_MAX), index=True)
    name          = Column(Unicode(STR_MAX), index=True)
    short_description = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    source        = Column(Unicode(STR_MAX))
    match_count   = Column(Integer)
    approved      = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('sector', 'name'),)
    

class DescriptionDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def get_description(self, tpe, sector, name, watson_lookup=False):
        if tpe == 'career':
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
            entity = self.query(table) \
                         .filter(table.sector == sector,
                                 table.name == name) \
                         .first()
        if entity is not None:
            return dict_from_row(entity, pkeys=False)
        if not watson_lookup:
            return None

        r = requests.get(conf.WATSON_CONCEPT_INSIGHTS_GRAPH_URL+'label_search',
                         params={'query' : name,
                                 'concept_fields' : '{"link":1}',
                                 'prefix' : 'false',
                                 'limit' : '1'},
                         auth=(conf.WATSON_USERNAME, conf.WATSON_PASSWORD)) \
                    .json()
        try:
            match_count = len(r['matches'])
            label = r['matches'][0]['label']
        except:
            match_count = 0
            label = None
        if label:
            r = requests.get(conf.WATSON_CONCEPT_INSIGHTS_GRAPH_URL \
                             +'concepts/'+label.replace(' ', '_'),
                             auth=(conf.WATSON_USERNAME,
                                   conf.WATSON_PASSWORD)) \
                        .json()
            entity = table(sector=None,
                           name=name,
                           description=r.get('abstract', None),
                           url=r.get('link', None),
                           source='Wikipedia',
                           match_count=match_count,
                           checked=False)
        else:
            entity = table(sector=None,
                           name=name,
                           match_count=match_count,
                           checked=False)

        self.add(entity)
        self.flush()
        return dict_from_row(entity, pkeys=False)

