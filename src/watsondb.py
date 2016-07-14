__all__ = [
    'SkillDescription',
    'WatsonDB',
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

import conf
import requests
from logger import Logger


STR_MAX = 100000

SQLBase = sqlbase()


class Description(SQLBase):
    __tablename__ = 'description'
    id            = Column(BigInteger, primary_key=True)
    label         = Column(Unicode(STR_MAX), index=True)
    text          = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))

    __table_args__ = (UniqueConstraint('label'),)


class ConceptLabel(SQLBase):
    __tablename__ = 'concept_label'
    id            = Column(BigInteger, primary_key=True)
    concept       = Column(Unicode(STR_MAX), index=True)
    label         = Column(Unicode(STR_MAX),
                           ForeignKey('description.label'),
                           index=True)
    result_num    = Column(Integer)

    __table_args__ = (UniqueConstraint('concept', 'label'),)
    

class WatsonDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def get_descriptions(self, concept, lookup=False,
                         logger=Logger(None)):
        concept = concept.strip().lower()
        q = self.query(Description) \
                .join(ConceptLabel) \
                .filter(ConceptLabel.concept == concept) \
                .order_by(ConceptLabel.result_num)
        descriptions = [dict_from_row(desc, pkeys=False) for desc in q]

        if descriptions or not lookup:
            return descriptions

        # check if concept has been queried before
        q = self.query(ConceptLabel.id) \
                .filter(ConceptLabel.concept == concept)
        if q.first():
            return descriptions

        # retreive concept descriptions from Watson
        logger.log('Looking up concept {0:s}...'.format(repr(concept)))
        r = requests.get(conf.WATSON_CONCEPT_INSIGHTS_GRAPH_URL+'label_search',
                         params={'query' : concept,
                                 'concept_fields' : '{"link":1}',
                                 'prefix' : 'false',
                                 'limit' : '20'},
                         auth=(conf.WATSON_USERNAME, conf.WATSON_PASSWORD)) \
                    .json()
        logger.log('done.\n')
        matches = r.get('matches', [])

        if not matches:
            concept_label = ConceptLabel(concept=concept,
                                         label=None,
                                         result_num=None)
            self.add(concept_label)
            self.commit()
            return []

        for result_num, match in enumerate(matches):
            label = match['label'].replace(' ', '_')
            description = self.query(Description) \
                              .filter(Description.label == label) \
                              .first()
            if description is None:
                logger.log('Looking up label {0:s}...'.format(label))
                r = requests.get(
                    conf.WATSON_CONCEPT_INSIGHTS_GRAPH_URL+'concepts/'+label,
                    auth=(conf.WATSON_USERNAME, conf.WATSON_PASSWORD)).json()
                logger.log('done.\n')
                description = Description(label=label,
                                          text=r.get('abstract', None),
                                          url=r.get('link', None))
                self.add(description)
                self.flush()
                descriptions.append(dict_from_row(description, pkeys=False))
            concept_label = ConceptLabel(concept=concept,
                                         label=label,
                                         result_num=result_num)
            self.add(concept_label)
            
        self.commit()
        return descriptions
