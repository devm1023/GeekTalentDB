__all__ = [
    'LIProfile',
    'Experience',
    'Education',
    'LIProfileSkill',
    'ExperienceSkill',
    'Skill',
    'SkillWord',
    'Title',
    'TitleWord',
    'Sector',
    'Company',
    'CompanyWord',
    'Location',
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
from textnormalization import normalizedTitle, normalizedCompany, normalizedSkill
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
    totalExperience   = Column(Integer)
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
    experiences = relationship('Experience',
                               order_by='Experience.start',
                               cascade='all, delete-orphan')
    educations = relationship('Education',
                              order_by='Education.start',
                              cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class Experience(SQLBase):
    __tablename__ = 'experience'
    id             = Column(BigInteger, primary_key=True)
    datoinId       = Column(String(STR_MAX))
    liprofileId    = Column(BigInteger,
                            ForeignKey('liprofile.id'),
                            index=True)
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
    skills = relationship('ExperienceSkill',
                          order_by='ExperienceSkill.nrmSkill',
                          cascade='all, delete-orphan')

class Education(SQLBase):
    __tablename__ = 'education'
    id          = Column(BigInteger, primary_key=True)
    datoinId    = Column(String(STR_MAX))
    profileId   = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         index=True)
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
                         autoincrement=False)
    nrmName     = Column(Unicode(STR_MAX),
                         ForeignKey('skill.nrmName'),
                         primary_key=True,
                         autoincrement=False)
    rank        = Column(Float)

    skill = relationship('Skill')

class ExperienceSkill(SQLBase):
    __tablename__ = 'experience_skill'
    experienceId = Column(BigInteger,
                          ForeignKey('experience.id'),
                          primary_key=True,
                          autoincrement=False)
    nrmSkill     = Column(Unicode(STR_MAX),
                          ForeignKey('skill.nrmName'),
                          primary_key=True,
                          autoincrement=False)
    
    skill = relationship('Skill')
    
class Skill(SQLBase):
    __tablename__ = 'skill'
    nrmName         = Column(Unicode(STR_MAX),
                             primary_key=True,
                             autoincrement=False)
    name            = Column(Unicode(STR_MAX))
    liprofileCount  = Column(BigInteger)
    experienceCount = Column(BigInteger)

class SkillWord(SQLBase):
    __tablename__ = 'skill_word'
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
    name      = Column(Unicode(STR_MAX))
    liprofileCount  = Column(BigInteger)
    experienceCount = Column(BigInteger)

class TitleWord(SQLBase):
    __tablename__ = 'title_word'
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
    count     = Column(BigInteger)

class Company(SQLBase):
    __tablename__ = 'company'
    nrmName   = Column(Unicode(STR_MAX),
                       primary_key=True,
                       autoincrement=False)
    name      = Column(Unicode(STR_MAX))
    liprofileCount  = Column(BigInteger)
    experienceCount = Column(BigInteger)

class CompanyWord(SQLBase):
    __tablename__ = 'company_word'
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

class Institute(SQLBase):
    __tablename__ = 'institute'
    nrmName         = Column(Unicode(STR_MAX),
                             primary_key=True,
                             autoincrement=False)
    name            = Column(Unicode(STR_MAX))
    count           = Column(BigInteger)

class Degree(SQLBase):
    __tablename__ = 'degree'
    nrmName         = Column(Unicode(STR_MAX),
                             primary_key=True,
                             autoincrement=False)
    name            = Column(Unicode(STR_MAX))
    count           = Column(BigInteger)

class Subject(SQLBase):
    __tablename__ = 'subject'
    nrmName         = Column(Unicode(STR_MAX),
                             primary_key=True,
                             autoincrement=False)
    name            = Column(Unicode(STR_MAX))
    count           = Column(BigInteger)

class Word(SQLBase):
    __tablename__ = 'word'
    word                   = Column(Unicode(STR_MAX),
                                    primary_key=True)
    liprofileSkillCount    = Column(BigInteger)
    experienceSkillCount   = Column(BigInteger)
    liprofileTitleCount    = Column(BigInteger)
    experienceTitleCount   = Column(BigInteger)
    liprofileCompanyCount  = Column(BigInteger)
    experienceCompanyCount = Column(BigInteger)

class TitleSkill(SQLBase):
    __tablename__ = 'title_skill'
    nrmTitle        = Column(String(STR_MAX),
                             ForeignKey('title.nrmName'),
                             primary_key=True,
                             autoincrement=False,
                             index=True)
    nrmSkill        = Column(String(STR_MAX),
                             ForeignKey('skill.nrmName'),
                             primary_key=True,
                             autoincrement=False,
                             index=True)
    liprofileCount  = Column(BigInteger)
    experienceCount = Column(BigInteger)

class CompanySkill(SQLBase):
    __tablename__ = 'company_skill'
    nrmCompany      = Column(String(STR_MAX),
                             ForeignKey('company.nrmName'),
                             primary_key=True,
                             autoincrement=False,
                             index=True)
    nrmSkill        = Column(String(STR_MAX),
                             ForeignKey('skill.nrmName'),
                             primary_key=True,
                             autoincrement=False,
                             index=True)
    liprofileCount  = Column(BigInteger)
    experienceCount = Column(BigInteger)

class SkillSkill(SQLBase):
    __tablename__ = 'skill_skill'
    nrmSkill1        = Column(String(STR_MAX),
                              ForeignKey('skill.nrmName'),
                              primary_key=True,
                              autoincrement=False,
                              index=True)
    nrmSkill2        = Column(String(STR_MAX),
                              ForeignKey('skill.nrmName'),
                              primary_key=True,
                              autoincrement=False,
                              index=True)
    liprofileCount  = Column(BigInteger)
    experienceCount = Column(BigInteger)

class CareerStep(SQLBase):
    __tablename__ = 'career_step'
    id            = Column(BigInteger, primary_key=True)
    title1        = Column(String(STR_MAX),
                           ForeignKey('title.nrmName'),
                           index=True)
    title2        = Column(String(STR_MAX),
                           ForeignKey('title.nrmName'),
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
            for experience in liprofile['experiences']:
                experience.pop('id', None)
                experience.pop('liprofileId', None)
                experience.pop('title', None)
                experience.pop('company', None)
                if experience.get('skills', None) is not None:
                    skillnames = set()
                    newskills = []
                    for skill in experience['skills']:
                        if not skill:
                            continue
                        if skill not in skillnames:
                            newskills.append({'nrmSkill' : skill})
                            skillnames.add(skill)
                    experience['skills'] = newskills

        if liprofile.get('educations', None) is not None:
            for education in liprofile['educations']:
                education.pop('id', None)
                education.pop('institute', None)
                education.pop('degree', None)
                education.pop('subject', None)
                    
        return self.addFromDict(liprofile, LIProfile)

    def addCareerStep(self, title1, title2, title3):
        careerstep = self.query(CareerStep) \
                         .filter(CareerStep.title1 == title1,
                                 CareerStep.title2 == title2,
                                 CareerStep.title3 == title3) \
                         .first()
        if careerstep is None:
            careerstep = CareerStep(title1=title1,
                                    title2=title2,
                                    title3=title3,
                                    count=0)
            self.add(careerstep)

        careerstep.count += 1
        
    def findEntities(self, querytype, querytext):
        if querytype == 'title':
            wordtable = TitleWord
            wordcountcols = [Word.experienceTitleCount, Word.liprofileTitleCount]
            entitytable = Title
            entitycountcols = [Title.experienceCount, Title.liprofileCount]
            nrmfunc = normalizedTitle
        elif querytype == 'skill':
            wordtable = SkillWord
            wordcountcols = [Word.experienceSkillCount, Word.liprofileSkillCount]
            entitytable = Skill
            entitycountcols = [Skill.experienceCount, Skill.liprofileCount]
            nrmfunc = normalizedSkill
        elif querytype == 'company':
            wordtable = CompanyWord
            wordcountcols = [Word.experienceSkillCount, Word.liprofileSkillCount]
            entitytable = Company
            entitycountcols = [Company.experienceCount, Company.liprofileCount]
            nrmfunc = normalizedCompany


        words = nrmfunc(querytext).split()
        wordcounts = self.query(Word.word, *wordcountcols) \
                         .filter(Word.word.in_(words),
                                 or_(*[c > 0 for c in wordcountcols])) \
                         .all()
        wordcounts = [(w[0], sum(w[1:])) for w in wordcounts]
        wordcounts.sort(key=lambda x: x[1])
        entitynames = []
        for i in range(len(wordcounts), 0, -1):
            words = [wc[0] for wc in wordcounts[:i]]
            wordcountcol = func.count(wordtable.word).label('wordcount')
            q = self.query(wordtable.nrmName) \
                    .filter(wordtable.word.in_(words)) \
                    .group_by(wordtable.nrmName) \
                    .having(wordcountcol == len(words))
            entitynames = [name for name, in q]
            if entitynames:
                break

        entities = []
        for rec in self.query(entitytable.nrmName, *entitycountcols) \
                       .filter(entitytable.nrmName.in_(entitynames)):
            entities.append((rec[0], sum(rec[1:])))
        entities.sort(key=lambda x: -x[1])
        
        return entities, words


