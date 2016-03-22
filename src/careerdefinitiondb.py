__all__ = [
    'Career',
    'CareerSkill',
    'CareerCompany',
    'CareerSubject',
    'CareerInstitute',
    'PreviousTitle',
    'NextTitle',
    'EntityDescription',
    'CareerDefinitionDB',
    ]

import conf
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
    func, \
    or_
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry
import requests
import conf
from pprint import pprint


STR_MAX = 100000

SQLBase = sqlbase()


class SectorSkill(SQLBase):
    __tablename__ = 'sector_skill'
    id            = Column(BigInteger, primary_key=True)
    sectorName    = Column(Unicode(STR_MAX), index=True, nullable=False)
    skillName     = Column(Unicode(STR_MAX), index=True, nullable=False)
    totalCount    = Column(BigInteger)
    sectorCount   = Column(BigInteger)
    skillCount    = Column(BigInteger)
    count         = Column(BigInteger)
    relevanceScore = Column(Float)

    __table_args__ = (UniqueConstraint('sectorName', 'skillName'),)
    
    
class Career(SQLBase):
    __tablename__ = 'career'
    id            = Column(BigInteger, primary_key=True)
    title         = Column(Unicode(STR_MAX), index=True, nullable=False)
    linkedinSector = Column(Unicode(STR_MAX), index=True, nullable=False)
    totalCount    = Column(BigInteger)
    sectorCount   = Column(BigInteger)
    titleCount    = Column(BigInteger)
    count         = Column(BigInteger)
    relevanceScore = Column(Float)

    skillCloud = relationship('CareerSkill',
                              order_by='desc(CareerSkill.relevanceScore)',
                              cascade='all, delete-orphan')
    companyCloud = relationship('CareerCompany',
                              order_by='desc(CareerCompany.relevanceScore)',
                              cascade='all, delete-orphan')
    educationSubjects = relationship('CareerSubject',
                                     order_by='desc(CareerSubject.count)',
                                     cascade='all, delete-orphan')
    educationInstitutes = relationship('CareerInstitute',
                                       order_by='desc(CareerInstitute.count)',
                                       cascade='all, delete-orphan')
    previousTitles = relationship('PreviousTitle',
                                  order_by='desc(PreviousTitle.count)',
                                  cascade='all, delete-orphan')
    nextTitles = relationship('NextTitle',
                              order_by='desc(NextTitle.count)',
                              cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('linkedinSector', 'title'),)

class CareerSkill(SQLBase):
    __tablename__ = 'career_skill'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    skillName     = Column(Unicode(STR_MAX), index=True, nullable=False)
    totalCount    = Column(BigInteger)
    titleCount    = Column(BigInteger)
    skillCount    = Column(BigInteger)
    count         = Column(BigInteger)
    relevanceScore = Column(Float)
    
    __table_args__ = (UniqueConstraint('careerId', 'skillName'),)

class CareerCompany(SQLBase):
    __tablename__ = 'career_company'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    companyName   = Column(Unicode(STR_MAX), index=True, nullable=False)
    totalCount    = Column(BigInteger)
    titleCount    = Column(BigInteger)
    companyCount  = Column(BigInteger)
    count         = Column(BigInteger)
    relevanceScore = Column(Float)
    
    __table_args__ = (UniqueConstraint('careerId', 'companyName'),)
    
class CareerSubject(SQLBase):
    __tablename__ = 'career_subject'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    subjectName   = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    
    __table_args__ = (UniqueConstraint('careerId', 'subjectName'),)
    
class CareerInstitute(SQLBase):
    __tablename__ = 'career_institute'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    instituteName = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    
    __table_args__ = (UniqueConstraint('careerId', 'instituteName'),)

class PreviousTitle(SQLBase):
    __tablename__ = 'previous_title'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    previousTitle = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    
    __table_args__ = (UniqueConstraint('careerId', 'previousTitle'),)

class NextTitle(SQLBase):
    __tablename__ = 'next_title'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    nextTitle = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    
    __table_args__ = (UniqueConstraint('careerId', 'nextTitle'),)

class EntityDescription(SQLBase):
    __tablename__ = 'entity_description'
    id            = Column(BigInteger, primary_key=True)
    entityType    = Column(String(20), index=True)
    linkedinSector = Column(Unicode(STR_MAX), index=True)
    entityName    = Column(Unicode(STR_MAX), index=True)
    matchCount    = Column(Integer)
    description   = Column(Unicode(STR_MAX))
    descriptionUrl = Column(String(STR_MAX))
    descriptionSource = Column(Unicode(STR_MAX))
    edited        = Column(Boolean)
    
    __table_args__ = (UniqueConstraint('entityType', 'linkedinSector',
                                       'entityName'),)
    

class CareerDefinitionDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def _getEntityDescription(self, entityType, linkedinSector, entityName,
                              watsonLookup=False):
        entityTypes = [entityType]
        if entityType is not None:
            entityTypes.append(None)
        linkedinSectors = [linkedinSector]
        if linkedinSector is not None:
            linkedinSectors.append(None)

        entity = None
        for entityType in entityTypes:
            for linkedinSector in linkedinSectors:
                if entity is not None:
                    break
                entity = self.query(EntityDescription) \
                             .filter(EntityDescription.entityType \
                                     == entityType,
                                     EntityDescription.linkedinSector \
                                     == linkedinSector,
                                     EntityDescription.entityName \
                                     == entityName) \
                             .first()        
        if entity is not None:
            return dictFromRow(entity, pkeys=False)
        if not watsonLookup:
            return None

        r = requests.get(conf.WATSON_CONCEPT_INSIGHTS_GRAPH_URL+'label_search',
                         params={'query' : entityName,
                                 'concept_fields' : '{"link":1}',
                                 'prefix' : 'false',
                                 'limit' : '1'},
                         auth=(conf.WATSON_USERNAME, conf.WATSON_PASSWORD)) \
                    .json()
        try:
            matchCount = len(r['matches'])
            label = r['matches'][0]['label']
        except:
            matchCount = 0
            label = None
        if label:
            r = requests.get(conf.WATSON_CONCEPT_INSIGHTS_GRAPH_URL \
                             +'concepts/'+label.replace(' ', '_'),
                             auth=(conf.WATSON_USERNAME,
                                   conf.WATSON_PASSWORD)) \
                        .json()
            entity = EntityDescription(entityType=None,
                                       linkedinSector=None,
                                       entityName=entityName,
                                       description=r.get('abstract', None),
                                       descriptionUrl=r.get('link', None),
                                       descriptionSource='Wikipedia',
                                       matchCount=matchCount,
                                       edited=False)
        else:
            entity = EntityDescription(entityType=None,
                                       linkedinSector=None,
                                       entityName=entityName,
                                       matchCount=matchCount,
                                       edited=False)

        self.add(entity)
        self.flush()
        return dictFromRow(entity, pkeys=False)

    def addCareer(self, careerdict, update=False):
        self._getEntityDescription('title',
                                   careerdict['linkedinSector'],
                                   careerdict['title'],
                                   watsonLookup=True)
        for skilldict in careerdict.get('skillCloud', []):
            self._getEntityDescription(None, None, skilldict['skillName'],
                                       watsonLookup=True)
        for companydict in careerdict.get('companyCloud', []):
            self._getEntityDescription(None, None, companydict['companyName'],
                                       watsonLookup=True)
        for subjectdict in careerdict.get('educationSubjects', []):
            self._getEntityDescription(None, None, subjectdict['subjectName'],
                                       watsonLookup=True)
        for institutedict in careerdict.get('educationInstitutes', []):
            self._getEntityDescription(None, None,
                                       institutedict['instituteName'],
                                       watsonLookup=True)
        for titledict in careerdict.get('previousTitles', []):
            self._getEntityDescription(None, None, titledict['previousTitle'],
                                       watsonLookup=True)
        for titledict in careerdict.get('nextTitles', []):
            self._getEntityDescription(None, None, titledict['nextTitle'],
                                       watsonLookup=True)
        return self.addFromDict(careerdict, Career)

    def addSectorSkill(self, skilldict):
        return self.addFromDict(skilldict, SectorSkill)
    
    def getCareers(self, sectors, titles):
        results = []
        q = self.query(Career)
        if sectors:
            q = q.filter(Career.linkedinSector.in_(sectors))
        if titles:
            q = q.filter(Career.title.in_(titles))
        for career in q:
            careerdict = dictFromRow(career, pkeys=False, fkeys=False)
            careerdict['description'] \
                = self._getEntityDescription('title',
                                             career.linkedinSector,
                                             career.title)
            for skilldict in careerdict['skillCloud']:
                skilldict['description'] \
                    = self._getEntityDescription('skill',
                                                 career.linkedinSector,
                                                 skilldict['skillName'])
            for companydict in careerdict['companyCloud']:
                companydict['description'] \
                    = self._getEntityDescription('company',
                                                 career.linkedinSector,
                                                 companydict['companyName'])
            for subjectdict in careerdict['educationSubjects']:
                subjectdict['description'] \
                    = self._getEntityDescription('subject',
                                                 career.linkedinSector,
                                                 subjectdict['subjectName'])
            for institutedict in careerdict['educationInstitutes']:
                institutedict['description'] \
                    = self._getEntityDescription('institute',
                                                 career.linkedinSector,
                                                 institutedict['instituteName'])
            for titledict in careerdict['previousTitles']:
                titledict['description'] \
                    = self._getEntityDescription('title',
                                                 career.linkedinSector,
                                                 titledict['previousTitle'])
            for titledict in careerdict['nextTitles']:
                titledict['description'] \
                    = self._getEntityDescription('title',
                                                 career.linkedinSector,
                                                 titledict['nextTitle'])
            results.append(careerdict)

        return results
