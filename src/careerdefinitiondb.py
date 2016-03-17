__all__ = [
    'Career',
    'CareerSkill',
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
from pprint import pprint


STR_MAX = 100000

SQLBase = sqlbase()


class Career(SQLBase):
    __tablename__ = 'career'
    id            = Column(BigInteger, primary_key=True)
    title         = Column(Unicode(STR_MAX), index=True)
    linkedinSector = Column(Unicode(STR_MAX), index=True)
    description   = Column(Unicode(STR_MAX))
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
                           index=True)
    skillName     = Column(Unicode(STR_MAX), index=True)
    description   = Column(Unicode(STR_MAX))
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
                           index=True)
    companyName   = Column(Unicode(STR_MAX), index=True)
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
                           index=True)
    subjectName   = Column(Unicode(STR_MAX), index=True)
    count         = Column(BigInteger)

    __table_args__ = (UniqueConstraint('careerId', 'subjectName'),)
    
class CareerInstitute(SQLBase):
    __tablename__ = 'career_institute'
    id            = Column(BigInteger, primary_key=True)
    careerId      = Column(BigInteger, ForeignKey('career.id'),
                           index=True)
    instituteName = Column(Unicode(STR_MAX), index=True)
    count         = Column(BigInteger)
    
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
    

class CareerDefinitionDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addCareer(self, careerdict, update=False):
        id = self.query(Career.id) \
                 .filter(Career.linkedinSector \
                         == careerdict['linkedinSector'],
                         Career.title \
                         == careerdict['title']).first()
        if id is not None:
            if not update:
                return None
            else:
                id = id[0]
                careerdict['id'] = id

        for skill in careerdict.get('skillCloud', []):
            skillid = self.query(CareerSkill.id) \
                          .filter(CareerSkill.careerId == id,
                                  CareerSkill.skillName \
                                  == skill.get('skillName', None)) \
                          .first()
            if skillid is not None:
                skill['id'] = skillid[0]
                skill['careerId'] = id

        for company in careerdict.get('companyCloud', []):
            companyid = self.query(CareerCompany.id) \
                          .filter(CareerCompany.careerId == id,
                                  CareerCompany.companyName \
                                  == company.get('companyName', None)) \
                          .first()
            if companyid is not None:
                company['id'] = companyid[0]
                company['careerId'] = id
                
        for subject in careerdict.get('educationSubjects', []):
            subjectid = self.query(CareerSubject.id) \
                          .filter(CareerSubject.careerId == id,
                                  CareerSubject.subjectName \
                                  == subject.get('subjectName', None)) \
                          .first()
            if subjectid is not None:
                subject['id'] = subjectid[0]
                subject['careerId'] = id
                
        return self.addFromDict(careerdict, Career)

        for institute in careerdict.get('educationSubjects', []):
            instituteid = self.query(CareerSubject.id) \
                          .filter(CareerSubject.careerId == id,
                                  CareerSubject.instituteName \
                                  == institute.get('instituteName', None)) \
                          .first()
            if instituteid is not None:
                institute['id'] = instituteid[0]
                institute['careerId'] = id

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
