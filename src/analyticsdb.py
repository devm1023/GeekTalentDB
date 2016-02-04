__all__ = [
    'LIProfile',
    'LIExperience',
    'LIEducation',
    'LIProfileSkill',
    'LIExperienceSkill',
    'Skill',
    'SkillWord',
    'Title',
    'TitleWord',
    'Sector',
    'Company',
    'CompanyWord',
    'Location',
    'Postcode',
    'PostcodeWord',
    'Institute',
    'Degree',
    'Subject',
    'Word',
    'TitleSkill',
    'CompanySkill',
    'SkillSkill',
    'CareerStep',
    'AnalyticsDB',
    'skillScore',
    ]

import conf
from sqldb import *
from textnormalization import normalizedTitle, normalizedCompany, \
    normalizedSkill, splitNrmName
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


STR_MAX = 100000

SQLBase = sqlbase()

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    placeId           = Column(String(STR_MAX), ForeignKey('location.placeId'))
    rawTitle          = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX),
                               ForeignKey('title.nrmName'),
                               nullable=True,
                               index=True)
    titlePrefix       = Column(Unicode(STR_MAX))
    rawSector         = Column(Unicode(STR_MAX))
    nrmSector         = Column(Unicode(STR_MAX),
                               ForeignKey('sector.nrmName'),
                               nullable=True,
                               index=True)
    rawCompany        = Column(Unicode(STR_MAX))
    nrmCompany        = Column(Unicode(STR_MAX),
                               ForeignKey('company.nrmName'),
                               nullable=True,
                               index=True)
    description       = Column(Unicode(STR_MAX))
    connections       = Column(Integer)
    firstExperienceStart = Column(Date)
    lastExperienceStart  = Column(Date)
    lastExperienceEnd    = Column(Date)
    firstEducationStart  = Column(Date)
    lastEducationStart   = Column(Date)
    lastEducationEnd     = Column(Date)
    url               = Column(String(STR_MAX))
    pictureUrl        = Column(String(STR_MAX))
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    title = relationship('Title')
    sector = relationship('Sector')
    company = relationship('Company')
    location = relationship('Location')
    skills = relationship('LIProfileSkill',
                          order_by='LIProfileSkill.nrmName',
                          cascade='all, delete-orphan')
    experiences = relationship('LIExperience',
                               order_by='LIExperience.start',
                               cascade='all, delete-orphan')
    educations = relationship('LIEducation',
                              order_by='LIEducation.start',
                              cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class LIExperience(SQLBase):
    __tablename__ = 'liexperience'
    id             = Column(BigInteger, primary_key=True)
    datoinId       = Column(String(STR_MAX))
    liprofileId    = Column(BigInteger,
                            ForeignKey('liprofile.id'),
                            index=True)
    language       = Column(String(20))
    rawTitle       = Column(Unicode(STR_MAX))
    nrmTitle       = Column(Unicode(STR_MAX),
                            ForeignKey('title.nrmName'),
                            index=True)
    titlePrefix    = Column(Unicode(STR_MAX))
    rawCompany     = Column(Unicode(STR_MAX))
    nrmCompany     = Column(Unicode(STR_MAX),
                            ForeignKey('company.nrmName'),
                            index=True)
    placeId        = Column(String(STR_MAX), ForeignKey('location.placeId'))
    start          = Column(Date)
    end            = Column(Date)
    duration       = Column(Integer)
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(DateTime)

    title = relationship('Title')
    company = relationship('Company')
    skills = relationship('LIExperienceSkill',
                          order_by='LIExperienceSkill.nrmSkill',
                          cascade='all, delete-orphan')

class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
    id          = Column(BigInteger, primary_key=True)
    datoinId    = Column(String(STR_MAX))
    liprofileId = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         index=True)
    language       = Column(String(20))
    rawInstitute   = Column(Unicode(STR_MAX))
    nrmInstitute   = Column(Unicode(STR_MAX),
                            ForeignKey('institute.nrmName'),
                            index=True)
    rawdegree      = Column(Unicode(STR_MAX))
    nrmDegree      = Column(Unicode(STR_MAX),
                            ForeignKey('degree.nrmName'),
                            index=True)
    rawsubject     = Column(Unicode(STR_MAX))
    nrmSubject     = Column(Unicode(STR_MAX),
                            ForeignKey('subject.nrmName'),
                            index=True)
    start          = Column(Date)
    end            = Column(Date)
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(DateTime)

    institute = relationship('Institute')
    degree = relationship('Degree')
    subject = relationship('Subject')
    
    
class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    liprofileId = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         primary_key=True,
                         index=True,
                         autoincrement=False)
    nrmName     = Column(Unicode(STR_MAX),
                         ForeignKey('skill.nrmName'),
                         primary_key=True,
                         index=True,
                         autoincrement=False)
    reenforced  = Column(Boolean)

    skill = relationship('Skill')

class LIExperienceSkill(SQLBase):
    __tablename__ = 'liexperience_skill'
    liexperienceId = Column(BigInteger,
                            ForeignKey('liexperience.id'),
                            primary_key=True,
                            index=True,
                            autoincrement=False)
    nrmSkill     = Column(Unicode(STR_MAX),
                          ForeignKey('skill.nrmName'),
                          primary_key=True,
                          index=True,
                          autoincrement=False)
    
    skill = relationship('Skill')

class INProfile(SQLBase):
    __tablename__ = 'inprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    placeId           = Column(String(STR_MAX), ForeignKey('location.placeId'))
    rawTitle          = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX),
                               ForeignKey('title.nrmName'),
                               nullable=True,
                               index=True)
    titlePrefix       = Column(Unicode(STR_MAX))
    rawCompany        = Column(Unicode(STR_MAX))
    nrmCompany        = Column(Unicode(STR_MAX),
                               ForeignKey('company.nrmName'),
                               nullable=True,
                               index=True)
    description       = Column(Unicode(STR_MAX))
    firstExperienceStart = Column(Date)
    lastExperienceStart  = Column(Date)
    lastExperienceEnd    = Column(Date)
    firstEducationStart  = Column(Date)
    lastEducationStart   = Column(Date)
    lastEducationEnd     = Column(Date)
    url               = Column(String(STR_MAX))
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    title = relationship('Title')
    company = relationship('Company')
    location = relationship('Location')
    skills = relationship('INProfileSkill',
                          order_by='INProfileSkill.nrmName',
                          cascade='all, delete-orphan')
    experiences = relationship('INExperience',
                               order_by='INExperience.start',
                               cascade='all, delete-orphan')
    educations = relationship('INEducation',
                              order_by='INEducation.start',
                              cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class INExperience(SQLBase):
    __tablename__ = 'inexperience'
    id             = Column(BigInteger, primary_key=True)
    datoinId       = Column(String(STR_MAX))
    inprofileId    = Column(BigInteger,
                            ForeignKey('inprofile.id'),
                            index=True)
    language       = Column(String(20))
    rawTitle       = Column(Unicode(STR_MAX))
    nrmTitle       = Column(Unicode(STR_MAX),
                            ForeignKey('title.nrmName'),
                            index=True)
    titlePrefix    = Column(Unicode(STR_MAX))
    rawCompany     = Column(Unicode(STR_MAX))
    nrmCompany     = Column(Unicode(STR_MAX),
                            ForeignKey('company.nrmName'),
                            index=True)
    placeId        = Column(String(STR_MAX), ForeignKey('location.placeId'))
    start          = Column(Date)
    end            = Column(Date)
    duration       = Column(Integer)
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(DateTime)

    title = relationship('Title')
    company = relationship('Company')
    skills = relationship('INExperienceSkill',
                          order_by='INExperienceSkill.nrmSkill',
                          cascade='all, delete-orphan')

class INEducation(SQLBase):
    __tablename__ = 'ineducation'
    id          = Column(BigInteger, primary_key=True)
    datoinId    = Column(String(STR_MAX))
    inprofileId = Column(BigInteger,
                         ForeignKey('inprofile.id'),
                         index=True)
    language       = Column(String(20))
    rawInstitute   = Column(Unicode(STR_MAX))
    nrmInstitute   = Column(Unicode(STR_MAX),
                            ForeignKey('institute.nrmName'),
                            index=True)
    rawdegree      = Column(Unicode(STR_MAX))
    nrmDegree      = Column(Unicode(STR_MAX),
                            ForeignKey('degree.nrmName'),
                            index=True)
    rawsubject     = Column(Unicode(STR_MAX))
    nrmSubject     = Column(Unicode(STR_MAX),
                            ForeignKey('subject.nrmName'),
                            index=True)
    start          = Column(Date)
    end            = Column(Date)
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(DateTime)

    institute = relationship('Institute')
    degree = relationship('Degree')
    subject = relationship('Subject')
    
class INProfileSkill(SQLBase):
    __tablename__ = 'inprofile_skill'
    inprofileId = Column(BigInteger,
                         ForeignKey('inprofile.id'),
                         primary_key=True,
                         index=True,
                         autoincrement=False)
    nrmName     = Column(Unicode(STR_MAX),
                         ForeignKey('skill.nrmName'),
                         primary_key=True,
                         index=True,
                         autoincrement=False)
    reenforced  = Column(Boolean)

    skill = relationship('Skill')

class INExperienceSkill(SQLBase):
    __tablename__ = 'inexperience_skill'
    inexperienceId = Column(BigInteger,
                            ForeignKey('inexperience.id'),
                            primary_key=True,
                            index=True,
                            autoincrement=False)
    nrmSkill     = Column(Unicode(STR_MAX),
                          ForeignKey('skill.nrmName'),
                          primary_key=True,
                          index=True,
                          autoincrement=False)
    
    skill = relationship('Skill')
    
class Skill(SQLBase):
    __tablename__ = 'skill'
    nrmName           = Column(Unicode(STR_MAX),
                               primary_key=True,
                               autoincrement=False)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    profileCount      = Column(BigInteger)
    experienceCount   = Column(BigInteger)

class SkillWord(SQLBase):
    __tablename__ = 'skill_word'
    language      = Column(String(20),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    word          = Column(Unicode(STR_MAX),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    nrmName       = Column(Unicode(STR_MAX),
                           ForeignKey('skill.nrmName'),
                           index=True,
                           primary_key=True,
                           autoincrement=False)

class Title(SQLBase):
    __tablename__ = 'title'
    nrmName   = Column(Unicode(STR_MAX),
                       primary_key=True,
                       autoincrement=False)
    language  = Column(String(20))
    name      = Column(Unicode(STR_MAX))
    profileCount    = Column(BigInteger)
    experienceCount = Column(BigInteger)

class TitleWord(SQLBase):
    __tablename__ = 'title_word'
    language      = Column(String(20),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    word          = Column(Unicode(STR_MAX),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    nrmName       = Column(Unicode(STR_MAX),
                           ForeignKey('title.nrmName'),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    
class Sector(SQLBase):
    __tablename__ = 'sector'
    nrmName   = Column(Unicode(STR_MAX),
                       primary_key=True,
                       autoincrement=False)
    name      = Column(Unicode(STR_MAX))
    liCount   = Column(BigInteger)

class Company(SQLBase):
    __tablename__ = 'company'
    nrmName   = Column(Unicode(STR_MAX),
                       primary_key=True,
                       autoincrement=False)
    language  = Column(String(20))
    name      = Column(Unicode(STR_MAX))
    profileCount    = Column(BigInteger)
    experienceCount = Column(BigInteger)

class CompanyWord(SQLBase):
    __tablename__ = 'company_word'
    language      = Column(String(20),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    word          = Column(Unicode(STR_MAX),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    nrmName       = Column(Unicode(STR_MAX),
                           ForeignKey('company.nrmName'),
                           index=True,
                           primary_key=True,
                           autoincrement=False)

class Location(SQLBase):
    __tablename__ = 'location'
    placeId   = Column(String(STR_MAX),
                       primary_key=True,
                       autoincrement=False)
    name      = Column(Unicode(STR_MAX))
    geo       = Column(Geometry('POINT'))

class Postcode(SQLBase):
    __tablename__ = 'postcode'
    id            = Column(BigInteger, primary_key=True)
    country       = Column(Unicode(STR_MAX))
    state         = Column(Unicode(STR_MAX))
    region        = Column(Unicode(STR_MAX))
    town          = Column(Unicode(STR_MAX))
    postcode      = Column(Unicode(STR_MAX))
    geo           = Column(Geometry('POINT'))

class PostcodeWord(SQLBase):
    __tablename__ = 'postcode_word'
    word          = Column(Unicode(STR_MAX),
                           primary_key=True,
                           index=True,
                           autoincrement=False)
    postcodeId    = Column(BigInteger,
                           ForeignKey('postcode.id'),
                           primary_key=True,
                           index=True,
                           autoincrement=False)
    type          = Column(String(20))
    country       = Column(Unicode(STR_MAX),
                           index=True)

class Institute(SQLBase):
    __tablename__ = 'institute'
    nrmName         = Column(Unicode(STR_MAX),
                             primary_key=True,
                             autoincrement=False)
    language        = Column(String(20))
    name            = Column(Unicode(STR_MAX))
    count           = Column(BigInteger)

class Degree(SQLBase):
    __tablename__ = 'degree'
    nrmName         = Column(Unicode(STR_MAX),
                             primary_key=True,
                             autoincrement=False)
    language        = Column(String(20))
    name            = Column(Unicode(STR_MAX))
    count           = Column(BigInteger)

class Subject(SQLBase):
    __tablename__ = 'subject'
    nrmName         = Column(Unicode(STR_MAX),
                             primary_key=True,
                             autoincrement=False)
    language        = Column(String(20))
    name            = Column(Unicode(STR_MAX))
    count           = Column(BigInteger)

class Word(SQLBase):
    __tablename__ = 'word'
    language                 = Column(String(20),
                                      primary_key=True)
    word                     = Column(Unicode(STR_MAX),
                                      primary_key=True)
    profileSkillCount        = Column(BigInteger)
    experienceSkillCount     = Column(BigInteger)
    profileTitleCount        = Column(BigInteger)
    experienceTitleCount     = Column(BigInteger)
    profileCompanyCount      = Column(BigInteger)
    experienceCompanyCount   = Column(BigInteger)

class TitleSkill(SQLBase):
    __tablename__ = 'title_skill'
    nrmTitle          = Column(String(STR_MAX),
                               ForeignKey('title.nrmName'),
                               primary_key=True,
                               autoincrement=False,
                               index=True)
    nrmSkill          = Column(String(STR_MAX),
                               ForeignKey('skill.nrmName'),
                               primary_key=True,
                               autoincrement=False,
                               index=True)
    liprofileCount    = Column(BigInteger)
    liexperienceCount = Column(BigInteger)

class CompanySkill(SQLBase):
    __tablename__ = 'company_skill'
    nrmCompany        = Column(String(STR_MAX),
                               ForeignKey('company.nrmName'),
                               primary_key=True,
                               autoincrement=False,
                               index=True)
    nrmSkill          = Column(String(STR_MAX),
                               ForeignKey('skill.nrmName'),
                               primary_key=True,
                               autoincrement=False,
                               index=True)
    liprofileCount    = Column(BigInteger)
    liexperienceCount = Column(BigInteger)

class SkillSkill(SQLBase):
    __tablename__ = 'skill_skill'
    nrmSkill1          = Column(String(STR_MAX),
                                ForeignKey('skill.nrmName'),
                                primary_key=True,
                                autoincrement=False,
                                index=True)
    nrmSkill2          = Column(String(STR_MAX),
                                ForeignKey('skill.nrmName'),
                                primary_key=True,
                                autoincrement=False,
                                index=True)
    liprofileCount    = Column(BigInteger)
    liexperienceCount = Column(BigInteger)

class CareerStep(SQLBase):
    __tablename__ = 'career_step'
    id            = Column(BigInteger, primary_key=True)
    titlePrefix1  = Column(String(STR_MAX),
                           index=True)
    title1        = Column(String(STR_MAX),
                           ForeignKey('title.nrmName'),
                           index=True)
    titlePrefix2  = Column(String(STR_MAX),
                           index=True)
    title2        = Column(String(STR_MAX),
                           ForeignKey('title.nrmName'),
                           index=True)
    titlePrefix3  = Column(String(STR_MAX),
                           index=True)
    title3        = Column(String(STR_MAX),
                           ForeignKey('title.nrmName'),
                           index=True)
    count         = Column(BigInteger)
    

def skillScore(coincidenceCount, categoryCount, skillCount, nrecords):
    """Measure how strongly a skill is associated with a certain category.

    Args:
      coincidenceCount (int): Number of times the skill appears in records of
        the desired category.
      categoryCount (int): The total number of records belonging to the category.
      skillCount (int): The total number of records associated with the skill.
      nrecords (int): The total number of records.

    Returns:
      float: A number between -1 and 1 measuring the strength of the relationship
        between the skill and the category. A value of 1 indicates a strong
        relationship, 0 means no relation, and -1 means that the skill and the
        category are mutually exclusive.

    """
    return coincidenceCount/categoryCount \
        - (skillCount-coincidenceCount)/(nrecords-categoryCount)


class AnalyticsDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addLIProfile(self, liprofile):
        """Add a LinkedIn profile to the database.

        Args:
          liprofile (dict): A ``dict`` describing the LinkedIn profile. It must
            contain the following fields:

              ``'datoinId'``
                The ID of the profile from DATOIN.

              ``'language'``
                The language of the profile.

              ``'name'``
                The name of the LinkedIn user.

              ``'placeId'``
                The Google Place ID for the user's location.

              ``'nrmTitle'``
                The normalized profile title.

              ``'nrmCompany'``
                The normalized name of the user's current company.

              ``'description'``
                The profile summary.

              ``'totalExperience'``
                The total work experience in days.

              ``'profileUrl'``
                The URL of the profile.

              ``'profilePictureUrl'``
                The URL of the profile picture.

              ``'indexedOn'``
                The date when the profile was indexed.

              ``'skills'``
                The skills declared by the user. This should be a list of 
                ``dict``s with the following fields:

                  ``'nrmName'``
                    The normalized name of the skill.

                  ``'rank'``
                    The rank of the skill.

              ``'experiences'``
                The work experiences of the user. This should be a list of
                ``dict``s with the following fields:

                  ``'datoinId'``
                    The ID of the experience record from DATOIN.

                  ``'title'``
                    The role/job title of the work experience.

                  ``'company'``
                    The name of the company where the person worked.

                  ``'placeId'``
                    The Google Place ID for the experience location.

                  ``'start'``
                    The start date of the work experience.

                  ``'end'``
                    The end date of the work experience.

                  ``'description'``
                    A free-text description of the work experience.

                  ``'indexedOn'``
                    The date when the record was indexed.

                  ``'skills'``
                    The skills that are explicitely mentioned in this experience
                    record. This should be a list of normalized skill names.
              
        Returns:
          The ``LIProfile`` object that was added to the database.

        """

        liprofile.pop('title', None)
        liprofile.pop('company', None)
        liprofile.pop('location', None)
        liprofile.pop('sector', None)
        language = liprofile.get('language', None)
        
        if liprofile.get('skills', None) is not None:
            skillnames = set()
            newskills = []
            for skill in liprofile['skills']:
                if not skill or not skill.get('nrmName', None):
                    continue
                skill.pop('liprofileId', None)
                skill.pop('skill', None)
                if skill['nrmName'] not in skillnames:
                    skillnames.add(skill['nrmName'])
                    newskills.append(skill)
            liprofile['skills'] = newskills

        if liprofile.get('experiences', None) is not None:
            for liexperience in liprofile['experiences']:
                liexperience.pop('id', None)
                liexperience.pop('liprofileId', None)
                liexperience.pop('title', None)
                liexperience.pop('company', None)
                liexperience['language'] = language
                if liexperience.get('skills', None) is not None:
                    skillnames = set()
                    newskills = []
                    for skill in liexperience['skills']:
                        if not skill:
                            continue
                        if skill not in skillnames:
                            newskills.append({'nrmSkill' : skill})
                            skillnames.add(skill)
                    liexperience['skills'] = newskills

        if liprofile.get('educations', None) is not None:
            for lieducation in liprofile['educations']:
                lieducation.pop('id', None)
                lieducation.pop('institute', None)
                lieducation.pop('degree', None)
                lieducation.pop('subject', None)
                lieducation['language'] = language
                    
        return self.addFromDict(liprofile, LIProfile)

    def addINProfile(self, inprofile):
        """Add a LinkedIn profile to the database.

        Args:
          inprofile (dict): A ``dict`` describing the LinkedIn profile. It must
            contain the following fields:

              ``'datoinId'``
                The ID of the profile from DATOIN.

              ``'language'``
                The language of the profile.

              ``'name'``
                The name of the LinkedIn user.

              ``'placeId'``
                The Google Place ID for the user's location.

              ``'nrmTitle'``
                The normalized profile title.

              ``'nrmCompany'``
                The normalized name of the user's current company.

              ``'description'``
                The profile summary.

              ``'totalExperience'``
                The total work experience in days.

              ``'profileUrl'``
                The URL of the profile.

              ``'profilePictureUrl'``
                The URL of the profile picture.

              ``'indexedOn'``
                The date when the profile was indexed.

              ``'skills'``
                The skills declared by the user. This should be a list of 
                ``dict``s with the following fields:

                  ``'nrmName'``
                    The normalized name of the skill.

                  ``'rank'``
                    The rank of the skill.

              ``'experiences'``
                The work experiences of the user. This should be a list of
                ``dict``s with the following fields:

                  ``'datoinId'``
                    The ID of the experience record from DATOIN.

                  ``'title'``
                    The role/job title of the work experience.

                  ``'company'``
                    The name of the company where the person worked.

                  ``'placeId'``
                    The Google Place ID for the experience location.

                  ``'start'``
                    The start date of the work experience.

                  ``'end'``
                    The end date of the work experience.

                  ``'description'``
                    A free-text description of the work experience.

                  ``'indexedOn'``
                    The date when the record was indexed.

                  ``'skills'``
                    The skills that are explicitely mentioned in this experience
                    record. This should be a list of normalized skill names.
              
        Returns:
          The ``INProfile`` object that was added to the database.

        """
        inprofile.pop('title', None)
        inprofile.pop('company', None)
        inprofile.pop('location', None)
        language = inprofile.get('language', None)
        
        if inprofile.get('skills', None) is not None:
            skillnames = set()
            newskills = []
            for skill in inprofile['skills']:
                if not skill or not skill.get('nrmName', None):
                    continue
                skill.pop('inprofileId', None)
                skill.pop('skill', None)
                if skill['nrmName'] not in skillnames:
                    skillnames.add(skill['nrmName'])
                    newskills.append(skill)
            inprofile['skills'] = newskills

        if inprofile.get('experiences', None) is not None:
            for inexperience in inprofile['experiences']:
                inexperience.pop('id', None)
                inexperience.pop('inprofileId', None)
                inexperience.pop('title', None)
                inexperience.pop('company', None)
                inexperience['language'] = language
                if inexperience.get('skills', None) is not None:
                    skillnames = set()
                    newskills = []
                    for skill in inexperience['skills']:
                        if not skill:
                            continue
                        if skill not in skillnames:
                            newskills.append({'nrmSkill' : skill})
                            skillnames.add(skill)
                    inexperience['skills'] = newskills

        if inprofile.get('educations', None) is not None:
            for ineducation in inprofile['educations']:
                ineducation.pop('id', None)
                ineducation.pop('institute', None)
                ineducation.pop('degree', None)
                ineducation.pop('subject', None)
                ineducation['language'] = language
                    
        return self.addFromDict(inprofile, INProfile)
    
    def addCareerStep(self,
                      prefix1, title1,
                      prefix2, title2,
                      prefix3, title3):
        careerstep = self.query(CareerStep) \
                         .filter(CareerStep.titlePrefix1 == prefix1,
                                 CareerStep.title1       == title1,
                                 CareerStep.titlePrefix2 == prefix2,
                                 CareerStep.title2       == title2,
                                 CareerStep.titlePrefix3 == prefix3,
                                 CareerStep.title3       == title3) \
                         .first()
        if careerstep is None:
            careerstep = CareerStep(titlePrefix1=prefix1,
                                    title1=title1,
                                    titlePrefix2=prefix2,
                                    title2=title2,
                                    titlePrefix3=prefix3,
                                    title3=title3,
                                    count=0)
            self.add(careerstep)

        careerstep.count += 1
        
    def findEntities(self, querytype, language, querytext, exact=False):
        if querytype == 'title':
            wordtable = TitleWord
            wordcountcols = [Word.liexperienceTitleCount,
                             Word.liprofileTitleCount]
            entitytable = Title
            entitycountcols = [Title.liexperienceCount, Title.liprofileCount]
            nrmfunc = normalizedTitle
        elif querytype == 'skill':
            wordtable = SkillWord
            wordcountcols = [Word.liexperienceSkillCount,
                             Word.liprofileSkillCount]
            entitytable = Skill
            entitycountcols = [Skill.liexperienceCount, Skill.liprofileCount]
            nrmfunc = normalizedSkill
        elif querytype == 'company':
            wordtable = CompanyWord
            wordcountcols = [Word.liexperienceCompanyCount,
                             Word.liprofileCompanyCount]
            entitytable = Company
            entitycountcols = [Company.liexperienceCount,
                               Company.liprofileCount]
            nrmfunc = normalizedCompany


        words = splitNrmName(nrmfunc(language, querytext))[1].split()
        words = list(set(words))
        wordcounts = self.query(Word.word, *wordcountcols) \
                         .filter(Word.word.in_(words),
                                 Word.language == language,
                                 or_(*[c > 0 for c in wordcountcols])) \
                         .all()
        if exact and len(wordcounts) < len(words):
            return [], [w[0] for w in wordcounts]
        wordcounts = [(w[0], sum(w[1:])) for w in wordcounts]
        wordcounts.sort(key=lambda x: x[1])
        entitynames = []
        for i in range(len(wordcounts), 0, -1):
            words = [wc[0] for wc in wordcounts[:i]]
            wordcountcol = func.count(wordtable.word).label('wordcount')
            q = self.query(wordtable.nrmName) \
                    .filter(wordtable.language == language,
                            wordtable.word.in_(words)) \
                    .group_by(wordtable.nrmName) \
                    .having(wordcountcol == len(words))
            entitynames = [name for name, in q]
            if entitynames or exact:
                break

        entities = []
        if entitynames:
            for rec in self.query(entitytable.nrmName,
                                  entitytable.name,
                                  *entitycountcols) \
                           .filter(entitytable.nrmName.in_(entitynames)):
                entities.append((rec[0], rec[1], sum(rec[2:])))
        entities.sort(key=lambda x: -x[-1])
        
        return entities, words


