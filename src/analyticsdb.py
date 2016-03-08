__all__ = [
    'LIProfile',
    'LIExperience',
    'LIEducation',
    'LIProfileSkill',
    'LIExperienceSkill',
    'INProfile',
    'INExperience',
    'INEducation',
    'INProfileSkill',
    'INExperienceSkill',
    'Entity',
    'Word',
    'EntityEntity',
    'Location',
    'Postcode',
    'PostcodeWord',
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


# LinkedIn

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    placeId           = Column(String(STR_MAX), ForeignKey('location.placeId'))
    rawTitle          = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX),
                               ForeignKey('entity.nrmName'),
                               index=True)
    titlePrefix       = Column(Unicode(STR_MAX))
    rawSector         = Column(Unicode(STR_MAX))
    nrmSector         = Column(Unicode(STR_MAX),
                               ForeignKey('entity.nrmName'),
                               index=True)
    rawCompany        = Column(Unicode(STR_MAX))
    nrmCompany        = Column(Unicode(STR_MAX),
                               ForeignKey('entity.nrmName'),
                               index=True)
    description       = Column(Unicode(STR_MAX))
    connections       = Column(Integer)
    firstExperienceStart = Column(DateTime)
    lastExperienceStart  = Column(DateTime)
    lastExperienceEnd    = Column(DateTime)
    firstEducationStart  = Column(DateTime)
    lastEducationStart   = Column(DateTime)
    lastEducationEnd     = Column(DateTime)
    url               = Column(String(STR_MAX))
    pictureUrl        = Column(String(STR_MAX))
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    title = relationship('Entity',
                         primaryjoin='LIProfile.nrmTitle==Entity.nrmName')
    sector = relationship('Entity',
                          primaryjoin='LIProfile.nrmSector==Entity.nrmName')
    company = relationship('Entity',
                           primaryjoin='LIProfile.nrmCompany==Entity.nrmName')
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
                            ForeignKey('entity.nrmName'),
                            index=True)
    titlePrefix    = Column(Unicode(STR_MAX))
    rawCompany     = Column(Unicode(STR_MAX))
    nrmCompany     = Column(Unicode(STR_MAX),
                            ForeignKey('entity.nrmName'),
                            index=True)
    placeId        = Column(String(STR_MAX), ForeignKey('location.placeId'))
    start          = Column(DateTime)
    end            = Column(DateTime)
    duration       = Column(Integer)
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(DateTime)

    title = relationship('Entity',
                         primaryjoin='LIExperience.nrmTitle==Entity.nrmName')
    company = relationship('Entity',
                         primaryjoin='LIExperience.nrmCompany==Entity.nrmName')
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
                            ForeignKey('entity.nrmName'),
                            index=True)
    rawdegree      = Column(Unicode(STR_MAX))
    nrmDegree      = Column(Unicode(STR_MAX),
                            ForeignKey('entity.nrmName'),
                            index=True)
    rawsubject     = Column(Unicode(STR_MAX))
    nrmSubject     = Column(Unicode(STR_MAX),
                            ForeignKey('entity.nrmName'),
                            index=True)
    start          = Column(DateTime)
    end            = Column(DateTime)
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(DateTime)

    institute \
        = relationship('Entity',
                       primaryjoin='LIEducation.nrmInstitute==Entity.nrmName')
    degree = relationship('Entity',
                          primaryjoin='LIEducation.nrmDegree==Entity.nrmName')
    subject = relationship('Entity',
                           primaryjoin='LIEducation.nrmSubject==Entity.nrmName')
    
class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    liprofileId = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         primary_key=True,
                         index=True,
                         autoincrement=False)
    nrmName     = Column(Unicode(STR_MAX),
                         ForeignKey('entity.nrmName'),
                         primary_key=True,
                         index=True,
                         autoincrement=False)
    reenforced  = Column(Boolean)

    skill = relationship('Entity')

class LIExperienceSkill(SQLBase):
    __tablename__ = 'liexperience_skill'
    liexperienceId = Column(BigInteger,
                            ForeignKey('liexperience.id'),
                            primary_key=True,
                            index=True,
                            autoincrement=False)
    nrmSkill     = Column(Unicode(STR_MAX),
                          ForeignKey('entity.nrmName'),
                          primary_key=True,
                          index=True,
                          autoincrement=False)
    
    skill = relationship('Entity')


# Indeed
    
class INProfile(SQLBase):
    __tablename__ = 'inprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    placeId           = Column(String(STR_MAX), ForeignKey('location.placeId'))
    rawTitle          = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX),
                               ForeignKey('entity.nrmName'),
                               nullable=True,
                               index=True)
    titlePrefix       = Column(Unicode(STR_MAX))
    rawCompany        = Column(Unicode(STR_MAX))
    nrmCompany        = Column(Unicode(STR_MAX),
                               ForeignKey('entity.nrmName'),
                               nullable=True,
                               index=True)
    description       = Column(Unicode(STR_MAX))
    firstExperienceStart = Column(DateTime)
    lastExperienceStart  = Column(DateTime)
    lastExperienceEnd    = Column(DateTime)
    firstEducationStart  = Column(DateTime)
    lastEducationStart   = Column(DateTime)
    lastEducationEnd     = Column(DateTime)
    url               = Column(String(STR_MAX))
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    title = relationship('Entity',
                         primaryjoin='INProfile.nrmTitle==Entity.nrmName')
    company = relationship('Entity',
                           primaryjoin='INProfile.nrmCompany==Entity.nrmName')
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
                            ForeignKey('entity.nrmName'),
                            index=True)
    titlePrefix    = Column(Unicode(STR_MAX))
    rawCompany     = Column(Unicode(STR_MAX))
    nrmCompany     = Column(Unicode(STR_MAX),
                            ForeignKey('entity.nrmName'),
                            index=True)
    placeId        = Column(String(STR_MAX), ForeignKey('location.placeId'))
    start          = Column(DateTime)
    end            = Column(DateTime)
    duration       = Column(Integer)
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(DateTime)

    title = relationship('Entity',
                         primaryjoin='INExperience.nrmTitle==Entity.nrmName')
    company = relationship('Entity', primaryjoin \
                           ='INExperience.nrmCompany==Entity.nrmName')
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
                            ForeignKey('entity.nrmName'),
                            index=True)
    rawdegree      = Column(Unicode(STR_MAX))
    nrmDegree      = Column(Unicode(STR_MAX),
                            ForeignKey('entity.nrmName'),
                            index=True)
    rawsubject     = Column(Unicode(STR_MAX))
    nrmSubject     = Column(Unicode(STR_MAX),
                            ForeignKey('entity.nrmName'),
                            index=True)
    start          = Column(DateTime)
    end            = Column(DateTime)
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(DateTime)

    institute \
        = relationship('Entity',
                       primaryjoin='INEducation.nrmInstitute==Entity.nrmName')
    degree = relationship('Entity',
                          primaryjoin='INEducation.nrmDegree==Entity.nrmName')
    subject = relationship('Entity',
                           primaryjoin='INEducation.nrmSubject==Entity.nrmName')
    
class INProfileSkill(SQLBase):
    __tablename__ = 'inprofile_skill'
    inprofileId = Column(BigInteger,
                         ForeignKey('inprofile.id'),
                         primary_key=True,
                         index=True,
                         autoincrement=False)
    nrmName     = Column(Unicode(STR_MAX),
                         ForeignKey('entity.nrmName'),
                         primary_key=True,
                         index=True,
                         autoincrement=False)
    reenforced  = Column(Boolean)

    skill = relationship('Entity')

class INExperienceSkill(SQLBase):
    __tablename__ = 'inexperience_skill'
    inexperienceId = Column(BigInteger,
                            ForeignKey('inexperience.id'),
                            primary_key=True,
                            index=True,
                            autoincrement=False)
    nrmSkill     = Column(Unicode(STR_MAX),
                          ForeignKey('entity.nrmName'),
                          primary_key=True,
                          index=True,
                          autoincrement=False)
    
    skill = relationship('Entity')


# entities
    
class Entity(SQLBase):
    __tablename__ = 'entity'
    nrmName           = Column(Unicode(STR_MAX),
                               primary_key=True,
                               autoincrement=False)
    type              = Column(String(20), index=True)
    source            = Column(String(20), index=True)
    language          = Column(String(20), index=True)
    name              = Column(Unicode(STR_MAX))
    profileCount      = Column(BigInteger, index=True)
    subDocumentCount  = Column(BigInteger, index=True)

class Word(SQLBase):
    __tablename__ = 'word'
    word          = Column(Unicode(STR_MAX),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    nrmName       = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrmName'),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    type          = Column(String(20), index=True)
    source        = Column(String(20), index=True)
    language      = Column(String(20), index=True)
    

class EntityEntity(SQLBase):
    __tablename__ = 'entity_entity'
    nrmName1           = Column(Unicode(STR_MAX),
                                primary_key=True,
                                autoincrement=False)
    nrmName2           = Column(Unicode(STR_MAX),
                                primary_key=True,
                                autoincrement=False)
    source            = Column(String(20), index=True)
    language          = Column(String(20), index=True)
    type1             = Column(String(20), index=True)
    type2             = Column(String(20), index=True)
    profileCount      = Column(BigInteger)
    subDocumentCount  = Column(BigInteger)


# locations
    
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
    country       = Column(Unicode(STR_MAX))

# class CareerStep(SQLBase):
#     __tablename__ = 'career_step'
#     id            = Column(BigInteger, primary_key=True)
#     titlePrefix1  = Column(String(STR_MAX),
#                            index=True)
#     title1        = Column(String(STR_MAX),
#                            ForeignKey('title.nrmName'),
#                            index=True)
#     titlePrefix2  = Column(String(STR_MAX),
#                            index=True)
#     title2        = Column(String(STR_MAX),
#                            ForeignKey('title.nrmName'),
#                            index=True)
#     titlePrefix3  = Column(String(STR_MAX),
#                            index=True)
#     title3        = Column(String(STR_MAX),
#                            ForeignKey('title.nrmName'),
#                            index=True)
#     count         = Column(BigInteger)
    

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
          liprofile (dict): A ``dict`` describing the LinkedIn profile. Valid
            fields are:

              * all columns of LIProfile *
              skills (list of dict)
                nrmName
              experiences (list of dict)
                * all columns of LIExperience *
                skills (list of str)
                  nrmName
              education (list of dict)
                * all columns of LIEducation *
              
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

              * all columns of INProfile *
              skills (list of dict)
                nrmName
              experiences (list of dict)
                * all columns of INExperience *
                skills (list of str)
                  nrmName
              education (list of dict)
                * all columns of INEducation *
              
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
    
    # def addCareerStep(self,
    #                   prefix1, title1,
    #                   prefix2, title2,
    #                   prefix3, title3):
    #     careerstep = self.query(CareerStep) \
    #                      .filter(CareerStep.titlePrefix1 == prefix1,
    #                              CareerStep.title1       == title1,
    #                              CareerStep.titlePrefix2 == prefix2,
    #                              CareerStep.title2       == title2,
    #                              CareerStep.titlePrefix3 == prefix3,
    #                              CareerStep.title3       == title3) \
    #                      .first()
    #     if careerstep is None:
    #         careerstep = CareerStep(titlePrefix1=prefix1,
    #                                 title1=title1,
    #                                 titlePrefix2=prefix2,
    #                                 title2=title2,
    #                                 titlePrefix3=prefix3,
    #                                 title3=title3,
    #                                 count=0)
    #         self.add(careerstep)

    #     careerstep.count += 1
        
    def findEntities(self, querytype, source, language, querytext,
                     minProfileCount=None, minSubDocumentCount=None,
                     exact=False):
        if querytype == 'title':
            nrmfunc = normalizedTitle
        elif querytype == 'skill':
            nrmfunc = normalizedSkill
        elif querytype == 'company':
            nrmfunc = normalizedCompany
        else:
            raise ValueError('Unsupported query type `{0:s}`.' \
                             .format(querytype))

        if exact:
            entitynames = [nrmfunc(source, language, querytext)]
        else:
            words = splitNrmName(nrmfunc(source, language, querytext))[-1] \
                    .split()
            words = list(set(words))
            if not words:
                return [], []

            q = self.query(Word.nrmName)
            filters = [Word.type == querytype,
                       Word.source == source,
                       Word.language == language,
                       Word.word == words[0]]
            for word in words[1:]:
                wordAlias = aliased(Word)
                q = q.join(wordAlias, wordAlias.nrmName == Word.nrmName)
                filters.append(wordAlias.word == word)
            q = q.filter(*filters).distinct()
            entitynames = [entity for entity, in q]

        entities = []
        if entitynames:
            q = self.query(Entity.nrmName,
                           Entity.name,
                           Entity.profileCount,
                           Entity.subDocumentCount) \
                    .filter(Entity.nrmName.in_(entitynames))
            if minProfileCount is not None:
                q = q.filter(Entity.profileCount >= minProfileCount)
            if minSubDocumentCount is not None:
                q = q.filter(Entity.subDocumentCount >= minSubDocumentCount)
            for nrmName, name, profileCount, subDocumentCount in q:
                if not profileCount:
                    profileCount = 0
                if not subDocumentCount:
                    subDocumentCount = 0
                entities.append((nrmName, name, profileCount, subDocumentCount))
                
        return entities


