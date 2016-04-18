__all__ = [
    'Career',
    'CareerSkill',
    'CareerCompany',
    'CareerSubject',
    'CareerInstitute',
    'PreviousTitle',
    'NextTitle',
    'SalaryBin',
    'SalaryHistoryPoint',
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
    sector_name   = Column(Unicode(STR_MAX), index=True, nullable=False)
    skill_name    = Column(Unicode(STR_MAX), index=True, nullable=False)
    total_count   = Column(BigInteger)
    sector_count  = Column(BigInteger)
    skill_count   = Column(BigInteger)
    count         = Column(BigInteger)
    relevance_score = Column(Float)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('sector_name', 'skill_name'),)


class Career(SQLBase):
    __tablename__ = 'career'
    id            = Column(BigInteger, primary_key=True)
    title         = Column(Unicode(STR_MAX), index=True, nullable=False)
    linkedin_sector = Column(Unicode(STR_MAX), index=True, nullable=False)
    total_count   = Column(BigInteger)
    sector_count  = Column(BigInteger)
    title_count   = Column(BigInteger)
    count         = Column(BigInteger)
    relevance_score = Column(Float)
    education_subjects_total = Column(BigInteger)
    education_institutes_total = Column(BigInteger)
    previous_titles_total = Column(BigInteger)
    next_titles_total = Column(BigInteger)
    visible       = Column(Boolean, nullable=False)

    skill_cloud = relationship('CareerSkill',
                               order_by='desc(CareerSkill.relevance_score)',
                               cascade='all, delete-orphan')
    company_cloud = relationship('CareerCompany',
                                 order_by='desc(CareerCompany.relevance_score)',
                                 cascade='all, delete-orphan')
    education_subjects = relationship('CareerSubject',
                                      order_by='desc(CareerSubject.count)',
                                      cascade='all, delete-orphan')
    education_institutes = relationship('CareerInstitute',
                                        order_by='desc(CareerInstitute.count)',
                                        cascade='all, delete-orphan')
    previous_titles = relationship('PreviousTitle',
                                   order_by='desc(PreviousTitle.count)',
                                   cascade='all, delete-orphan')
    next_titles = relationship('NextTitle',
                               order_by='desc(NextTitle.count)',
                               cascade='all, delete-orphan')
    salary_bins = relationship('SalaryBin',
                               order_by='SalaryBin.lower_bound',
                               cascade='all, delete-orphan')
    salary_history_points = relationship('SalaryHistoryPoint',
                                         order_by='SalaryHistoryPoint.date',
                                         cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('linkedin_sector', 'title'),)

class CareerSkill(SQLBase):
    __tablename__ = 'career_skill'
    id            = Column(BigInteger, primary_key=True)
    career_id     = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    skill_name    = Column(Unicode(STR_MAX), index=True, nullable=False)
    total_count   = Column(BigInteger)
    title_count   = Column(BigInteger)
    skill_count   = Column(BigInteger)
    count         = Column(BigInteger)
    relevance_score = Column(Float)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('career_id', 'skill_name'),)

class CareerCompany(SQLBase):
    __tablename__ = 'career_company'
    id            = Column(BigInteger, primary_key=True)
    career_id     = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    company_name  = Column(Unicode(STR_MAX), index=True, nullable=False)
    total_count   = Column(BigInteger)
    title_count   = Column(BigInteger)
    company_count = Column(BigInteger)
    count         = Column(BigInteger)
    relevance_score = Column(Float)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('career_id', 'company_name'),)

class CareerSubject(SQLBase):
    __tablename__ = 'career_subject'
    id            = Column(BigInteger, primary_key=True)
    career_id     = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    subject_name  = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('career_id', 'subject_name'),)

class CareerInstitute(SQLBase):
    __tablename__ = 'career_institute'
    id            = Column(BigInteger, primary_key=True)
    career_id     = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    institute_name = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('career_id', 'institute_name'),)

class PreviousTitle(SQLBase):
    __tablename__ = 'previous_title'
    id            = Column(BigInteger, primary_key=True)
    career_id     = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    previous_title = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('career_id', 'previous_title'),)

class NextTitle(SQLBase):
    __tablename__ = 'next_title'
    id            = Column(BigInteger, primary_key=True)
    career_id     = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    next_title = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('career_id', 'next_title'),)

class SalaryBin(SQLBase):
    __tablename__ = 'salary_bin'
    id            = Column(BigInteger, primary_key=True)
    career_id     = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    lower_bound   = Column(Float)
    upper_bound   = Column(Float)
    count         = Column(Integer)

class SalaryHistoryPoint(SQLBase):
    __tablename__ = 'salary_history_point'
    id            = Column(BigInteger, primary_key=True)
    career_id     = Column(BigInteger, ForeignKey('career.id'),
                           index=True, nullable=False)
    date          = Column(Date)
    salary        = Column(Float)
    

class EntityDescription(SQLBase):
    __tablename__ = 'entity_description'
    id            = Column(BigInteger, primary_key=True)
    entity_type   = Column(String(20), index=True)
    linkedin_sector = Column(Unicode(STR_MAX), index=True)
    entity_name   = Column(Unicode(STR_MAX), index=True)
    match_count   = Column(Integer)
    description   = Column(Unicode(STR_MAX))
    description_url = Column(String(STR_MAX))
    description_source = Column(Unicode(STR_MAX))
    edited        = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('entity_type', 'linkedin_sector',
                                       'entity_name'),)


class CareerDefinitionDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def _get_entity_description(self, entity_type, linkedin_sector, entity_name,
                              watson_lookup=False):
        entity_types = [entity_type]
        if entity_type is not None:
            entity_types.append(None)
        linkedin_sectors = [linkedin_sector]
        if linkedin_sector is not None:
            linkedin_sectors.append(None)

        entity = None
        for entity_type in entity_types:
            for linkedin_sector in linkedin_sectors:
                if entity is not None:
                    break
                entity = self.query(EntityDescription) \
                             .filter(EntityDescription.entity_type \
                                     == entity_type,
                                     EntityDescription.linkedin_sector \
                                     == linkedin_sector,
                                     EntityDescription.entity_name \
                                     == entity_name) \
                             .first()
        if entity is not None:
            return dict_from_row(entity, pkeys=False)
        if not watson_lookup:
            return None

        r = requests.get(conf.WATSON_CONCEPT_INSIGHTS_GRAPH_URL+'label_search',
                         params={'query' : entity_name,
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
            entity = EntityDescription(entity_type=None,
                                       linkedin_sector=None,
                                       entity_name=entity_name,
                                       description=r.get('abstract', None),
                                       description_url=r.get('link', None),
                                       description_source='Wikipedia',
                                       match_count=match_count,
                                       edited=False)
        else:
            entity = EntityDescription(entity_type=None,
                                       linkedin_sector=None,
                                       entity_name=entity_name,
                                       match_count=match_count,
                                       edited=False)

        self.add(entity)
        self.flush()
        return dict_from_row(entity, pkeys=False)

    def get_descriptions(self, career):
        sector = career.linkedin_sector
        self._get_entity_description(
            'title', sector, career.title, watson_lookup=True)
        for skill in career.skill_cloud:
            self._get_entity_description(
                'skill', sector, skill.skill_name, watson_lookup=True)
        for company in career.company_cloud:
            self._get_entity_description(
                'company', sector, company.company_name, watson_lookup=True)
        for subject in career.education_subjects:
            self._get_entity_description(
                'subject', sector, subject.subject_name, watson_lookup=True)
        for institute in career.education_institutes:
            self._get_entity_description(
                'institute', sector, institute.institute_name,
                watson_lookup=True)
        for title in career.previous_titles:
            self._get_entity_description(
                'title', sector, title.previous_title, watson_lookup=True)
        for title in career.next_titles:
            self._get_entity_description(
                'title', sector, title.next_title, watson_lookup=True)
    
    def add_career(self, careerdict, get_descriptions=False):
        career = self.add_from_dict(
            careerdict, Career, protect=['visible',
                                         ('skill_cloud', 'visible'),
                                         ('education_subjects', 'visible'),
                                         ('education_institutes', 'visible'),
                                         ('previous_titles', 'visible'),
                                         ('next_titles', 'visible')])
        if get_descriptions:
            self.get_descriptions(career)
        return career

    def add_sector_skill(self, skilldict):
        return self.add_from_dict(skilldict, SectorSkill,
                                  protect=['visible'])

    def get_careers(self, sectors, titles):
        results = []
        q = self.query(Career) \
                .filter(Career.visible)
        if sectors:
            q = q.filter(Career.linkedin_sector.in_(sectors))
        if titles:
            q = q.filter(Career.title.in_(titles))
        for career in q:
            career.skill_cloud \
                = [s for s in career.skill_cloud if s.visible]
            career.company_cloud \
                = [s for s in career.company_cloud if s.visible]
            career.education_subjects \
                = [s for s in career.education_subjects if s.visible]
            career.education_institutes \
                = [s for s in career.education_institutes if s.visible]
            career.previous_titles \
                = [s for s in career.previous_titles if s.visible]
            career.next_titles \
                = [s for s in career.next_titles if s.visible]

            careerdict = dict_from_row(career, pkeys=False, fkeys=False)
            if careerdict['salary_bins']:
                wsum = 0.0
                totalcount = 0
                for salary_bin in careerdict['salary_bins']:
                    if salary_bin['upper_bound'] is None:
                        salary = salary_bin['lower_bound']
                    else:
                        salary = 0.5*(salary_bin['lower_bound'] \
                                      + salary_bin['upper_bound'])
                    wsum += salary*salary_bin['count']
                    totalcount += salary_bin['count']
                careerdict['average_salary'] = wsum/totalcount
            else:
                careerdict['average_salary'] = None
            
            careerdict['description'] = self._get_entity_description(
                    'title', career.linkedin_sector, career.title)
            for skilldict in careerdict['skill_cloud']:
                skilldict['description'] = self._get_entity_description(
                    'skill', career.linkedin_sector, skilldict['skill_name'])
            for companydict in careerdict['company_cloud']:
                companydict['description'] = self._get_entity_description(
                    'company', career.linkedin_sector,
                    companydict['company_name'])
            for subjectdict in careerdict['education_subjects']:
                subjectdict['description'] = self._get_entity_description(
                    'subject', career.linkedin_sector,
                    subjectdict['subject_name'])
            for institutedict in careerdict['education_institutes']:
                institutedict['description'] = self._get_entity_description(
                    'institute', career.linkedin_sector,
                    institutedict['institute_name'])
            for titledict in careerdict['previous_titles']:
                titledict['description'] = self._get_entity_description(
                    'title', career.linkedin_sector,
                    titledict['previous_title'])
            for titledict in careerdict['next_titles']:
                titledict['description'] = self._get_entity_description(
                    'title', career.linkedin_sector, titledict['next_title'])
            results.append(careerdict)

        return results
