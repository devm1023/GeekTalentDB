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
    'CareerDefinitionDB',
    ]

import conf
from dbtools import *
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

SQLBase = declarative_base()


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
    
    
class CareerDefinitionDBSession(Session):
    def add_career(self, careerdict):
        career = self.add_from_dict(
            careerdict, Career, protect=['visible',
                                         ('skill_cloud', 'visible'),
                                         ('education_subjects', 'visible'),
                                         ('education_institutes', 'visible'),
                                         ('previous_titles', 'visible'),
                                         ('next_titles', 'visible')])
        return career

    def add_sector_skill(self, skilldict):
        return self.add_from_dict(skilldict, SectorSkill,
                                  protect=['visible'])

    def get_sectors(self, sectors, description_db=None):
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

            if description_db is not None:
                sectordict['description'] = description_db.get_description(
                    'sector', None, sector.name)
                for skilldict in sectordict['skill_cloud']:
                    skilldict['description'] = description_db.get_description(
                        'skill', sector.name, skilldict['skill_name'])

            results.append(sectordict)
        return results
    
    def get_careers(self, sector, titles, description_db=None):
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

            if description_db is not None:
                careerdict['description'] = description_db.get_description(
                        'career', sector, career.title)
                for skilldict in careerdict['skill_cloud']:
                    skilldict['description'] = description_db.get_description(
                        'skill', sector, skilldict['skill_name'])

            results.append(careerdict)
        return results


class CareerDefinitionDB(Session):
    def __init__(self, url=conf.CAREERDEFINITION_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)

