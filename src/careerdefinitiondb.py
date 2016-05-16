__all__ = [
    'Sector',
    'SectorSkill',
    'SectorCompany',
    'SectorSubject',
    'SectorInstitute',
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
from windowquery import collapse
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


class Sector(SQLBase):
    __tablename__ = 'sector'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    total_count   = Column(BigInteger)
    education_subjects_total = Column(BigInteger)
    education_institutes_total = Column(BigInteger)
    visible       = Column(Boolean, nullable=False)

    skill_cloud = relationship('SectorSkill',
                               order_by='desc(SectorSkill.relevance_score)',
                               cascade='all, delete-orphan')
    company_cloud = relationship('SectorCompany',
                                 order_by='desc(SectorCompany.relevance_score)',
                                 cascade='all, delete-orphan')
    education_subjects = relationship('SectorSubject',
                                      order_by='desc(SectorSubject.count)',
                                      cascade='all, delete-orphan')
    education_institutes = relationship('SectorInstitute',
                                        order_by='desc(SectorInstitute.count)',
                                        cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('name'),)


class SectorSkill(SQLBase):
    __tablename__ = 'sector_skill'
    id            = Column(BigInteger, primary_key=True)
    sector_id     = Column(BigInteger,  ForeignKey('sector.id'),
                           index=True, nullable=False)
    skill_name    = Column(Unicode(STR_MAX), index=True, nullable=False)
    total_count   = Column(BigInteger)
    sector_count  = Column(BigInteger)
    skill_count   = Column(BigInteger)
    count         = Column(BigInteger)
    relevance_score = Column(Float)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('sector_id', 'skill_name'),)


class SectorCompany(SQLBase):
    __tablename__ = 'sector_company'
    id            = Column(BigInteger, primary_key=True)
    sector_id     = Column(BigInteger,  ForeignKey('sector.id'),
                           index=True, nullable=False)
    company_name  = Column(Unicode(STR_MAX), index=True, nullable=False)
    total_count   = Column(BigInteger)
    sector_count  = Column(BigInteger)
    company_count = Column(BigInteger)
    count         = Column(BigInteger)
    relevance_score = Column(Float)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('sector_id', 'company_name'),)


class SectorSubject(SQLBase):
    __tablename__ = 'sector_subject'
    id            = Column(BigInteger, primary_key=True)
    sector_id     = Column(BigInteger, ForeignKey('sector.id'),
                           index=True, nullable=False)
    subject_name  = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('sector_id', 'subject_name'),)


class SectorInstitute(SQLBase):
    __tablename__ = 'sector_institute'
    id            = Column(BigInteger, primary_key=True)
    sector_id     = Column(BigInteger, ForeignKey('sector.id'),
                           index=True, nullable=False)
    institute_name = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
    visible       = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('sector_id', 'institute_name'),)


class Career(SQLBase):
    __tablename__ = 'career'
    id            = Column(BigInteger, primary_key=True)
    sector_id     = Column(BigInteger, ForeignKey('sector.id'),
                           index=True, nullable=False)
    title         = Column(Unicode(STR_MAX), index=True, nullable=False)
    count         = Column(BigInteger)
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

    __table_args__ = (UniqueConstraint('sector_id', 'title'),)


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
    short_description = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    description_url = Column(String(STR_MAX))
    description_source = Column(Unicode(STR_MAX))
    edited        = Column(Boolean, nullable=False)

    __table_args__ = (UniqueConstraint('entity_type', 'linkedin_sector',
                                       'entity_name'),)


def _remove_invisibles(d):
    d.pop('visible', None)
    for key, val in d.items():
        if isinstance(val, list):
            if val and isinstance(val[0], dict) and 'visible' in val[0]:
                d[key] = [_remove_invisibles(e) for e in val if e['visible']]
        elif isinstance(val, dict):
            if 'visible' in val and val['visible'] == False:
                d[key] = None
            else:
                d[key] = _remove_invisibles(val)
            
    return d

def _average_salary(bins):
    wsum = 0.0
    totalcount = 0
    for salary_bin in bins:
        if salary_bin['upper_bound'] is None:
            salary = salary_bin['lower_bound']
        else:
            salary = 0.5*(salary_bin['lower_bound'] \
                          + salary_bin['upper_bound'])
        wsum += salary*salary_bin['count']
        totalcount += salary_bin['count']
    if totalcount > 0:
        return wsum/totalcount
    else:
        return None
    
    
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
        sector, = self.query(Sector.name) \
                      .filter(Sector.id == career.sector_id) \
                      .first()
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

    def get_sectors(self, sectors):
        results = []
        q= self.query(Sector) \
               .filter(Sector.visible)
        if sectors:
            q = q.filter(Sector.name.in_(sectors))
        for sector in q:
            sectordict = dict_from_row(sector, pkeys=False, fkeys=False)
            sectordict = _remove_invisibles(sectordict)

            q2 = self.query(Career.title, Career.count, SalaryBin) \
                     .outerjoin(SalaryBin) \
                     .filter(Career.visible,
                             Career.sector_id == sector.id) \
                     .order_by(Career.title, Career.count)
            sectordict['careers'] = []
            wsum = 0.0
            totalcount = 0
            for title, count, salary_bins in collapse(q2, on=2):
                salary_bins = [dict_from_row(b) for b, in salary_bins \
                               if b is not None]
                salary = _average_salary(salary_bins)
                if salary is not None:
                    wsum += count*salary
                    totalcount += count
                sectordict['careers'].append(title)
            sectordict['average_salary'] = None
            if totalcount > 0 and wsum > 0.0:
                sectordict['average_salary'] = wsum/totalcount

            sectordict['description'] = self._get_entity_description(
                    'sector', None, sectordict['name'])
            for skilldict in sectordict['skill_cloud']:
                skilldict['description'] = self._get_entity_description(
                    'skill', sector.name, skilldict['skill_name'])
            for companydict in sectordict['company_cloud']:
                companydict['description'] = self._get_entity_description(
                    'company', sector.name, companydict['company_name'])
            for subjectdict in sectordict['education_subjects']:
                subjectdict['description'] = self._get_entity_description(
                    'subject', sector.name, subjectdict['subject_name'])
            for institutedict in sectordict['education_institutes']:
                institutedict['description'] = self._get_entity_description(
                    'institute', sector.name, institutedict['institute_name'])

            results.append(sectordict)
        return results
    
    def get_careers(self, sector, titles):
        sector_id = self.query(Sector.id) \
                        .filter(Sector.name == sector) \
                        .first()
        if sector_id is None:
            return []
        else:
            sector_id = sector_id[0]
            
        results = []
        q = self.query(Career) \
                .filter(Career.visible,
                        Career.sector_id == sector_id)
        if titles:
            q = q.filter(Career.title.in_(titles))
        for career in q:
            careerdict = dict_from_row(career, pkeys=False, fkeys=False)
            careerdict = _remove_invisibles(careerdict)

            careerdict['average_salary'] = None
            if careerdict['salary_bins']:
                careerdict['average_salary'] \
                    = _average_salary(careerdict['salary_bins'])

            for point in careerdict['salary_history_points']:
                point['date'] = point['date'].strftime('%Y-%m')
            
            careerdict['description'] = self._get_entity_description(
                    'title', sector, career.title)
            for skilldict in careerdict['skill_cloud']:
                skilldict['description'] = self._get_entity_description(
                    'skill', sector, skilldict['skill_name'])
            for companydict in careerdict['company_cloud']:
                companydict['description'] = self._get_entity_description(
                    'company', sector, companydict['company_name'])
            for subjectdict in careerdict['education_subjects']:
                subjectdict['description'] = self._get_entity_description(
                    'subject', sector, subjectdict['subject_name'])
            for institutedict in careerdict['education_institutes']:
                institutedict['description'] = self._get_entity_description(
                    'institute', sector, institutedict['institute_name'])
            for titledict in careerdict['previous_titles']:
                titledict['description'] = self._get_entity_description(
                    'title', sector, titledict['previous_title'])
            for titledict in careerdict['next_titles']:
                titledict['description'] = self._get_entity_description(
                    'title', sector, titledict['next_title'])
            results.append(careerdict)

        return results
