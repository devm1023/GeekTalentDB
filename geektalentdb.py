__all__ = [
    'create_engine',
    'sessionmaker',
    'SQLBase',
    'LIProfile',
    'Skill',
    'Jobtitle',
    'Location',
    'LocationName',
    'Experience',
    'ExperienceSkill',
    'ExperienceJobtitle',
    'LIProfileSkill',
    'LIProfileJobtitle',
    'GeekTalentDB',
    ]

import conf
from phrasematch import stem, tokenize, matchStems
from datetime import datetime
import re
import numpy as np

from sqlalchemy import \
    create_engine, \
    Column, \
    UniqueConstraint, \
    ForeignKey, \
    Integer, \
    BigInteger, \
    Unicode, \
    UnicodeText, \
    String, \
    Text, \
    Date, \
    Float, \
    func
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

import requests

def UTF8String(*args, **kwargs):
    # if conf.USE_MYSQL:
    #     return Unicode(*args, collation='utf8_bin', **kwargs)
    return Unicode(*args, **kwargs)

def UTF8Text(*args, **kwargs):
    # if conf.USE_MYSQL:
    #     return UnicodeText(*args, collation='utf8_bin', **kwargs)
    return UnicodeText(*args, **kwargs)


SQLBase = declarative_base()


# entities

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id = Column(BigInteger, primary_key=True)
    parent_id = Column(String(1024))
    name = Column(UTF8String(255))
    location_id = Column(BigInteger, ForeignKey('location.id'))
    url = Column(String(1024))
    
    skills = relationship('LIProfileSkill')
    experiences = relationship('Experience', order_by='Experience.startdate')
    jobtitles = relationship('LIProfileJobtitle')

class Location(SQLBase):
    __tablename__ = 'location'
    id = Column(BigInteger, primary_key=True)
    name = Column(UTF8String(255))
    geo = Column(Geometry('POINT'))

class LocationName(SQLBase):
    __tablename__ = 'location_name'
    nname = Column(UTF8String(255), primary_key=True)
    location_id = Column(BigInteger, ForeignKey('location.id'))

class Skill(SQLBase):
    __tablename__ = 'skill'
    id = Column(BigInteger, primary_key=True)
    nname = Column(UTF8String(255))

class SkillName(SQLBase):
    __tablename__ = 'skill_name'
    skill_id = Column(BigInteger,
                      ForeignKey('skill.id'),
                      primary_key=True)
    id = Column(BigInteger,
                primary_key=True)
    name = Column(UTF8String(255))
    count = Column(Integer)

class Jobtitle(SQLBase):
    __tablename__ = 'jobtitle'
    id = Column(BigInteger, primary_key=True)
    nname = Column(UTF8String(255))

class JobtitleName(SQLBase):
    __tablename__ = 'jobtitle_name'
    jobtitle_id = Column(BigInteger,
                         ForeignKey('jobtitle.id'),
                         primary_key=True)
    id = Column(BigInteger,
                primary_key=True)
    name = Column(UTF8String(255))
    count = Column(Integer)

class Company(SQLBase):
    __tablename__ = 'company'
    id = Column(BigInteger, primary_key=True)
    nname = Column(UTF8String(255))

class CompanyName(SQLBase):
    __tablename__ = 'company_name'
    company_id = Column(BigInteger,
                        ForeignKey('company.id'),
                        primary_key=True)
    id = Column(BigInteger,
                primary_key=True)
    name = Column(UTF8String(255))
    count = Column(Integer)

class Experience(SQLBase):
    __tablename__ = 'experience'
    liprofile_id = Column(BigInteger, ForeignKey('liprofile.id'))
    id = Column(BigInteger, primary_key=True)
    company_id = Column(BigInteger, ForeignKey('company.id'))
    startdate = Column(Date)
    enddate = Column(Date)
    duration = Column(Float)
    title = Column(UTF8String(255))
    description = Column(UTF8Text())

    skills = relationship('ExperienceSkill')
    jobtitles = relationship('ExperienceJobtitle')


# associations

class ExperienceSkill(SQLBase):
    __tablename__ = 'experience_skill'
    experience_id = Column(BigInteger,
                           ForeignKey('experience.id'),
                           primary_key=True)
    skill_id = Column(BigInteger,
                      ForeignKey('skill.id'),
                      primary_key=True)

class ExperienceJobtitle(SQLBase):
    __tablename__ = 'experience_jobtitle'
    experience_id = Column(BigInteger,
                           ForeignKey('experience.id'),
                           primary_key=True,
                           index=True)
    jobtitle_id = Column(BigInteger,
                         ForeignKey('jobtitle.id'),
                         primary_key=True,
                         index=True)

class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    liprofile_id = Column(BigInteger,
                          ForeignKey('liprofile.id'),
                          primary_key=True,
                        index=True)
    skill_id = Column(BigInteger,
                      ForeignKey('skill.id'),
                      primary_key=True,
                      index=True)
    rank = Column(Float)

class LIProfileJobtitle(SQLBase):
    __tablename__ = 'liprofile_jobtitle'
    liprofile_id = Column(BigInteger,
                          ForeignKey('liprofile.id'),
                          primary_key=True,
                        index=True)
    jobtitle_id = Column(BigInteger,
                         ForeignKey('jobtitle.id'),
                         primary_key=True,
                         index=True)
    
    

impossibleyear = 3000
defaultstartdate = datetime(year=impossibleyear, month=1, day=1)
defaultenddate = datetime(year=impossibleyear, month=12, day=31)


class GeekTalentDB:
    def __init__(self, url=None, session=None, engine=None):
        if session is None and engine is None and url is None:
            raise ValueError('One of url, session, or engine must be specified')
        if session is None:
            if engine is None:
                engine = create_engine(url)
            session = sessionmaker(bind=engine)()
        self.session = session
        self.query = session.query
        self.flush = session.flush
        self.commit = session.commit

    def drop_all(self):
        SQLBase.metadata.drop_all(self.session.bind)

    def create_all(self):
        SQLBase.metadata.create_all(self.session.bind)

    def add_skill(self, name):
        nname = stem(name)
        nname.sort()
        nname = ' '.join(nname)

        skill = self.query(Skill).filter(Skill.nname == nname).first()
        if not skill:
            skill = Skill(nname=nname)
            self.session.add(skill)
            self.flush()

        skillname = self.query(SkillName) \
                        .filter(SkillName.skill_id == skill.id) \
                        .filter(SkillName.name == name) \
                        .first()
        if not skillname:
            skillname = SkillName(skill_id=skill.id, name=name, count=1)
            self.session.add(skillname)
        else:
            skillname.count += 1
        self.flush()
        
        return skill

    def add_jobtitle(self, name):
        nname = stem(name, removebrackets=True)
        if not nname:
            return None
        nname = ' '.join(nname)

        jobtitle = self.query(Jobtitle) \
                       .filter(Jobtitle.nname == nname).first()
        if not jobtitle:
            jobtitle = Jobtitle(nname=nname)
            self.session.add(jobtitle)
            self.flush()

        jobtitlename = self.query(JobtitleName) \
                           .filter(JobtitleName.jobtitle_id == jobtitle.id) \
                           .filter(JobtitleName.name == name) \
                           .first()
        if not jobtitlename:
            jobtitlename = JobtitleName(jobtitle_id=jobtitle.id,
                                        name=name, count=1)
            self.session.add(jobtitlename)
        else:
            jobtitlename.count += 1
        self.flush()
            
        return jobtitle

    def add_company(self, name):
        nname = tokenize(name, removebrackets=True)
        if not nname:
            return None
        nname = ' '.join(nname)

        company = self.query(Company) \
                      .filter(Company.nname == nname).first()
        if not company:
            company = Company(nname=nname)
            self.session.add(company)
            self.flush()

        companyname = self.query(CompanyName) \
                          .filter(CompanyName.company_id == company.id) \
                          .filter(CompanyName.name == name) \
                          .first()
        if not companyname:
            companyname = CompanyName(company_id=company.id,
                                      name=name, count=1)
            self.session.add(companyname)
        else:
            companyname.count += 1
        self.flush()
            
        return company

    def add_location(self, name):
        nname = ' '.join(name.lower().split())
        locationname = self.query(LocationName) \
                           .filter(LocationName.nname == nname).first()
        if not locationname:
            r = requests.get(conf.MAPS_API,
                             params={'address' : nname}).json()
            if len(r['results']) != 1:
                return Location()
            lat = r['results'][0]['geometry']['location']['lat']
            lon = r['results'][0]['geometry']['location']['lng']
            address = r['results'][0]['formatted_address']

            # format address
            address = address.split(', ')
            if address:
                for i, s in enumerate(address[:-1]):
                    while len(address) > i+1 and address[i+1] == s:
                        del address[i+1]
            address = ', '.join(address)

            # find location
            lat = round(lat, conf.LATLON_DIGITS)
            lon = round(lon, conf.LATLON_DIGITS)
            lon1 = lon-conf.LATLON_DELTA
            lon2 = lon+conf.LATLON_DELTA
            lat1 = lat+conf.LATLON_DELTA
            lat2 = lat-conf.LATLON_DELTA
            pointstr = 'POINT({0:f} {1:f})'.format(lon, lat)
            polystr = 'POLYGON(({0:f} {1:f},{2:f} {1:f},{2:f} {3:f},{0:f} {3:f},{0:f} {1:f}))' \
                      .format(lon1, lat1, lon2, lat2)
            location = self.query(Location) \
                           .filter(func.ST_contains(polystr, Location.geo)) \
                           .first()
            if not location:
                location = Location(geo=pointstr, name=address)
                self.session.add(location)
                self.flush()
                
            locationname = LocationName(nname=nname, location_id=location.id)
            self.session.add(locationname)
            self.flush()
        else:
            location = self.query(Location) \
                           .filter(Location.id == locationname.location_id) \
                           .one()

        return location

    
    def delete_experienceskills(self, experience_ids):
        if hasattr(experience_ids, '__len__'):
            ids = tuple(experience_ids)
        else:
            ids = (experience_ids,)
        if ids:
            self.query(ExperienceSkill) \
                .filter(ExperienceSkill.experience_id.in_(ids)) \
                .delete(synchronize_session='fetch')
            self.flush()

    def add_experience(self, liprofile_id, companyname, startdate, enddate,
                       jobtitlename, description):
        # determine duration of work experience
        duration = None
        if startdate is not None and enddate is not None:
            if startdate < enddate:
                duration = (enddate - startdate).days

        # add company
        company = self.add_company(companyname)

        # add experience record
        experience = Experience(liprofile_id=liprofile_id,
                                company_id=company.id,
                                startdate=startdate,
                                enddate=enddate,
                                duration=duration,
                                title=jobtitlename,
                                description=description)
        self.session.add(experience)
        self.flush()

        # parse and add job title(s)
        if jobtitlename:
            pos = jobtitlename.find(' - ')
            if pos > 0:
                jobtitlename = jobtitlename[:pos]
            elif pos == 0:
                jobtitlename = jobtitlename[3:]
        if jobtitlename:
            jobtitlenames = re.split('( / )|(, )|(\\. )', jobtitlename)[0::4]
            jobtitleids = set()
            experiencejobtitles = []
            for t in jobtitlenames:
                jobtitle = self.add_jobtitle(t)
                if jobtitle and jobtitle.id not in jobtitleids:
                    jobtitleids.add(jobtitle.id)
                    experiencejobtitles.append(
                        ExperienceJobtitle(experience_id=experience.id,
                                           jobtitle_id=jobtitle.id))
            experience.jobtitles = experiencejobtitles
            self.flush()

        return experience

    def add_liprofile(self, parent_id, name, locationname, url,
                      skills, experiencedicts):
        # add location
        location = self.add_location(locationname)

        # add profile
        liprofile = LIProfile(parent_id=parent_id,
                              name=name,
                              location_id=location.id,
                              url=url)
        self.session.add(liprofile)
        self.flush()

        # add skills
        liprofileskills = []
        skillids = set()
        for skillname in skills:
            skill = self.add_skill(skillname)
            if skill.id not in skillids:
                skillids.add(skill.id)
                liprofileskills.append(
                    LIProfileSkill(liprofile_id=liprofile.id,
                                   skill_id=skill.id,
                                   rank=0.0))
        liprofile.skills = liprofileskills
        self.flush()

        # add experiences
        experiences = [self.add_experience(liprofile.id,
                                           experience['company'],
                                           experience['startdate'],
                                           experience['enddate'],
                                           experience['title'],
                                           experience['description']) \
                       for experience in experiencedicts]

        # compute skill ranks
        skills = [
            self.query(Skill).filter(Skill.id == ps.skill_id) \
                             .one() for ps in liprofileskills]
        descriptions = [' '.join([e.title, e.description]) for e in experiences]
        descriptionstems = list(map(stem, descriptions))
        skillstems = [s.nname.split() for s in skills]
        matches = (matchStems(skillstems, descriptionstems) > \
                   conf.SKILL_MATCHING_THRESHOLD)
        ranks = np.zeros(len(skills))
        for iexperience, experience in enumerate(experiences):
            experienceskills = []
            for iskill, skill in enumerate(skills):
                if matches[iskill, iexperience]:
                    if experience.duration:
                        duration = experience.duration
                    else:
                        duration = 365
                    ranks[iskill] += duration/365.0
                    experienceskills.append(
                        ExperienceSkill(experience_id=experience.id,
                                        skill_id=skill.id))
            experience.skills = experienceskills
        for liprofileskill, rank in zip(liprofileskills, ranks):
            liprofileskill.rank = rank
        
        return liprofile
