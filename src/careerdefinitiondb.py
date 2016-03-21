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
    sectorName    = Column(Unicode(STR_MAX), index=True)
    skillName     = Column(Unicode(STR_MAX), index=True)
    totalCount    = Column(BigInteger)
    sectorCount   = Column(BigInteger)
    skillCount    = Column(BigInteger)
    count         = Column(BigInteger)
    relevanceScore = Column(Float)

    __table_args__ = (UniqueConstraint('sectorName', 'skillName'),)
    
    
class Career(SQLBase):
    __tablename__ = 'career'
    id            = Column(BigInteger, primary_key=True)
    title         = Column(Unicode(STR_MAX), index=True)
    linkedinSector = Column(Unicode(STR_MAX), index=True)
    descriptionId = Column(BigInteger, ForeignKey('entity_description.id'))
    totalCount    = Column(BigInteger)
    sectorCount   = Column(BigInteger)
    titleCount    = Column(BigInteger)
    count         = Column(BigInteger)
    relevanceScore = Column(Float)

    description = relationship('EntityDescription')
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
                           index=True)
    skillName     = Column(Unicode(STR_MAX), index=True)
    descriptionId = Column(BigInteger,
                           ForeignKey('entity_description.id'))
    totalCount    = Column(BigInteger)
    titleCount    = Column(BigInteger)
    skillCount    = Column(BigInteger)
    count         = Column(BigInteger)
    relevanceScore = Column(Float)
    
    description = relationship('EntityDescription')
    
    __table_args__ = (UniqueConstraint('careerId', 'skillName'),)

class CareerCompany(SQLBase):
    __tablename__ = 'career_company'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True)
    companyName   = Column(Unicode(STR_MAX), index=True)
    descriptionId = Column(BigInteger,
                           ForeignKey('entity_description.id'))
    totalCount    = Column(BigInteger)
    titleCount    = Column(BigInteger)
    companyCount  = Column(BigInteger)
    count         = Column(BigInteger)
    relevanceScore = Column(Float)
    
    description = relationship('EntityDescription')
    
    __table_args__ = (UniqueConstraint('careerId', 'companyName'),)
    
class CareerSubject(SQLBase):
    __tablename__ = 'career_subject'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True)
    subjectName   = Column(Unicode(STR_MAX), index=True)
    descriptionId = Column(BigInteger,
                           ForeignKey('entity_description.id'))
    count         = Column(BigInteger)

    description = relationship('EntityDescription')
    
    __table_args__ = (UniqueConstraint('careerId', 'subjectName'),)
    
class CareerInstitute(SQLBase):
    __tablename__ = 'career_institute'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True)
    instituteName = Column(Unicode(STR_MAX), index=True)
    descriptionId = Column(BigInteger,
                           ForeignKey('entity_description.id'))
    count         = Column(BigInteger)
    
    description = relationship('EntityDescription')
    
    __table_args__ = (UniqueConstraint('careerId', 'instituteName'),)

class PreviousTitle(SQLBase):
    __tablename__ = 'previous_title'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True)
    previousTitle = Column(Unicode(STR_MAX), index=True)
    count         = Column(BigInteger)
    
    __table_args__ = (UniqueConstraint('careerId', 'previousTitle'),)

class NextTitle(SQLBase):
    __tablename__ = 'next_title'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True)
    nextTitle = Column(Unicode(STR_MAX), index=True)
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

    def _getEntityId(self, entityType, linkedinSector, entityName):
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
            return entity.id
                
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
            matchCount = None
            label = None
        if not label:
            return None
        r = requests.get(conf.WATSON_CONCEPT_INSIGHTS_GRAPH_URL \
                         +'concepts/'+label.replace(' ', '_'),
                         auth=(conf.WATSON_USERNAME, conf.WATSON_PASSWORD)) \
                    .json()
        
        entity = EntityDescription(entityType=None,
                                   linkedinSector=None,
                                   entityName=entityName,
                                   description=r.get('abstract', None),
                                   descriptionUrl=r.get('link', None),
                                   descriptionSource='Wikipedia',
                                   matchCount=matchCount,
                                   edited=False)
        self.add(entity)
        self.flush()
        return entity.id

    def addCareer(self, careerdict, update=False):
        linkedinSector = careerdict['linkedinSector']
        id = self.query(Career.id) \
                 .filter(Career.linkedinSector == linkedinSector,
                         Career.title == careerdict['title']) \
                 .first()
        if id is not None:
            if not update:
                return None
            else:
                id = id[0]
                careerdict['id'] = id

        careerdict['descriptionId'] \
            = self._getEntityId('title', linkedinSector, careerdict['title'])

        for skill in careerdict.get('skillCloud', []):
            skillid = self.query(CareerSkill.id) \
                          .filter(CareerSkill.careerId == id,
                                  CareerSkill.skillName \
                                  == skill.get('skillName', None)) \
                          .first()
            if skillid is not None:
                skill['id'] = skillid[0]
                skill['careerId'] = id
                skill['descriptionId'] \
                    = self._getEntityId('skill', linkedinSector,
                                        skill['skillName'])

        for company in careerdict.get('companyCloud', []):
            companyid = self.query(CareerCompany.id) \
                          .filter(CareerCompany.careerId == id,
                                  CareerCompany.companyName \
                                  == company.get('companyName', None)) \
                          .first()
            if companyid is not None:
                company['id'] = companyid[0]
                company['careerId'] = id
                company['descriptionId'] \
                    = self._getEntityId('company', linkedinSector,
                                        company['companyName'])
                
        for subject in careerdict.get('educationSubjects', []):
            subjectid = self.query(CareerSubject.id) \
                          .filter(CareerSubject.careerId == id,
                                  CareerSubject.subjectName \
                                  == subject.get('subjectName', None)) \
                          .first()
            if subjectid is not None:
                subject['id'] = subjectid[0]
                subject['careerId'] = id
                subject['descriptionId'] \
                    = self._getEntityId('subject', linkedinSector,
                                        subject['subjectName'])
                
        for institute in careerdict.get('educationInstitutes', []):
            instituteid = self.query(CareerInstitute.id) \
                          .filter(CareerInstitute.careerId == id,
                                  CareerInstitute.instituteName \
                                  == institute.get('instituteName', None)) \
                          .first()
            if instituteid is not None:
                institute['id'] = instituteid[0]
                institute['careerId'] = id
                institute['descriptionId'] \
                    = self._getEntityId('institute', linkedinSector,
                                        institute['instituteName'])

        for previousTitle in careerdict.get('previousTitles', []):
            titleid = self.query(PreviousTitle.id) \
                          .filter(PreviousTitle.careerId == id,
                                  PreviousTitle.previousTitle \
                                  == previousTitle.get('previousTitle', None)) \
                          .first()
            if titleid is not None:
                previousTitle['id'] = titleid[0]
                previousTitle['careerId'] = id

        for nextTitle in careerdict.get('nextTitles', []):
            titleid = self.query(NextTitle.id) \
                          .filter(NextTitle.careerId == id,
                                  NextTitle.nextTitle \
                                  == nextTitle.get('nextTitle', None)) \
                          .first()
            if titleid is not None:
                nextTitle['id'] = titleid[0]
                nextTitle['careerId'] = id
                
        return self.addFromDict(careerdict, Career)

    def addSectorSkill(self, skilldict):
        id = self.query(SectorSkill.id) \
                 .filter(SectorSkill.sectorName \
                         == skilldict.get('sectorName', None),
                         SectorSkill.skillName \
                         == skilldict.get('skillName', None)) \
                 .first()
        if id is not None:
            skilldict['id'] = id[0]
        return self.addFromDict(skilldict, SectorSkill)
    
    def getCareers(self, sectors, titles):
        results = []
        q = self.query(Career)
        if sectors:
            q = q.filter(Career.linkedinSector.in_(sectors))
        if titles:
            q = q.filter(Career.title.in_(titles))
        for career in q:
            careerdict = dictFromRow(career)
            careerdict.pop('id')
            for skilldict in careerdict['skillCloud']:
                skilldict.pop('id')
                skilldict.pop('careerId')
            for companydict in careerdict['companyCloud']:
                companydict.pop('id')
                companydict.pop('careerId')
            for subjectdict in careerdict['educationSubjects']:
                subjectdict.pop('id')
                subjectdict.pop('careerId')
            for institutedict in careerdict['educationInstitutes']:
                institutedict.pop('id')
                institutedict.pop('careerId')
            for titledict in careerdict['previousTitles']:
                titledict.pop('id')
                titledict.pop('careerId')
            for titledict in careerdict['nextTitles']:
                titledict.pop('id')
                titledict.pop('careerId')
            results.append(careerdict)

        return results
