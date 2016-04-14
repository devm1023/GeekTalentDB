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
    'skill_score',
    ]

import conf
from sqldb import *
from textnormalization import normalized_title, normalized_company, \
    normalized_skill, normalized_sector, normalized_institute, \
    normalized_subject, normalized_degree, split_nrm_name
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
from sqlalchemy.orm import relationship, aliased
from geoalchemy2 import Geometry


STR_MAX = 100000

SQLBase = sqlbase()


# LinkedIn

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX), index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    place_id      = Column(String(STR_MAX), ForeignKey('location.place_id'))
    raw_title     = Column(Unicode(STR_MAX))
    nrm_title     = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    title_prefix  = Column(Unicode(STR_MAX))
    raw_sector    = Column(Unicode(STR_MAX))
    nrm_sector    = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    raw_company   = Column(Unicode(STR_MAX))
    nrm_company   = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    description   = Column(Unicode(STR_MAX))
    connections   = Column(Integer)
    text_length   = Column(Integer)
    n_experiences = Column(Integer)
    first_experience_start = Column(DateTime)
    last_experience_start  = Column(DateTime)
    nrm_first_title = Column(Unicode(STR_MAX),
                             ForeignKey('entity.nrm_name'),
                             index=True)
    first_title_prefix = Column(Unicode(STR_MAX))
    nrm_first_company = Column(Unicode(STR_MAX),
                               ForeignKey('entity.nrm_name'),
                               index=True)
    nrm_curr_title = Column(Unicode(STR_MAX),
                            ForeignKey('entity.nrm_name'),
                            index=True)
    curr_title_prefix = Column(Unicode(STR_MAX))
    n_educations  = Column(Integer)
    first_education_start  = Column(DateTime)
    last_education_start   = Column(DateTime)
    last_education_end     = Column(DateTime)
    nrm_last_institute = Column(Unicode(STR_MAX),
                                ForeignKey('entity.nrm_name'),
                                index=True)
    nrm_last_subject = Column(Unicode(STR_MAX),
                              ForeignKey('entity.nrm_name'),
                              index=True)
    nrm_last_degree = Column(Unicode(STR_MAX),
                             ForeignKey('entity.nrm_name'),
                             index=True)
    url           = Column(String(STR_MAX))
    picture_url   = Column(String(STR_MAX))
    indexed_on    = Column(DateTime, index=True)
    crawled_on    = Column(DateTime, index=True)

    title = relationship('Entity',
                         primaryjoin='LIProfile.nrm_title==Entity.nrm_name')
    sector = relationship('Entity',
                          primaryjoin='LIProfile.nrm_sector==Entity.nrm_name')
    company = relationship('Entity',
                           primaryjoin='LIProfile.nrm_company==Entity.nrm_name')
    location = relationship('Location')
    skills = relationship('LIProfileSkill',
                          order_by='LIProfileSkill.nrm_name',
                          cascade='all, delete-orphan')
    experiences = relationship('LIExperience',
                               order_by='LIExperience.start',
                               cascade='all, delete-orphan')
    educations = relationship('LIEducation',
                              order_by='LIEducation.start',
                              cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('datoin_id'),)

class LIExperience(SQLBase):
    __tablename__ = 'liexperience'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX))
    liprofile_id  = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           index=True)
    language      = Column(String(20))
    raw_title     = Column(Unicode(STR_MAX))
    nrm_title     = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    title_prefix  = Column(Unicode(STR_MAX))
    raw_company   = Column(Unicode(STR_MAX))
    nrm_company   = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    place_id      = Column(String(STR_MAX), ForeignKey('location.place_id'))
    start         = Column(DateTime)
    end           = Column(DateTime)
    duration      = Column(Integer)
    description   = Column(Unicode(STR_MAX))
    indexed_on    = Column(DateTime)

    title = relationship('Entity',
                         primaryjoin='LIExperience.nrm_title==Entity.nrm_name')
    company = relationship(
        'Entity', primaryjoin='LIExperience.nrm_company==Entity.nrm_name')
    skills = relationship('LIExperienceSkill',
                          order_by='LIExperienceSkill.nrm_skill',
                          cascade='all, delete-orphan')

class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX))
    liprofile_id  = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           index=True)
    language      = Column(String(20))
    raw_institute = Column(Unicode(STR_MAX))
    nrm_institute = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    rawdegree     = Column(Unicode(STR_MAX))
    nrm_degree    = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    rawsubject    = Column(Unicode(STR_MAX))
    nrm_subject   = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    start         = Column(DateTime)
    end           = Column(DateTime)
    description   = Column(Unicode(STR_MAX))
    indexed_on    = Column(DateTime)

    institute = relationship(
        'Entity', primaryjoin='LIEducation.nrm_institute==Entity.nrm_name')
    degree = relationship(
        'Entity', primaryjoin='LIEducation.nrm_degree==Entity.nrm_name')
    subject = relationship(
        'Entity', primaryjoin='LIEducation.nrm_subject==Entity.nrm_name')

class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    liprofile_id  = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           primary_key=True,
                           index=True,
                           autoincrement=False)
    nrm_name      = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           primary_key=True,
                           index=True,
                           autoincrement=False)
    reenforced    = Column(Boolean)

    skill = relationship('Entity')

class LIExperienceSkill(SQLBase):
    __tablename__ = 'liexperience_skill'
    liexperience_id = Column(BigInteger,
                             ForeignKey('liexperience.id'),
                             primary_key=True,
                             index=True,
                             autoincrement=False)
    nrm_skill     = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           primary_key=True,
                           index=True,
                           autoincrement=False)

    skill = relationship('Entity')


# Indeed

class INProfile(SQLBase):
    __tablename__ = 'inprofile'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX), index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    place_id      = Column(String(STR_MAX), ForeignKey('location.place_id'))
    raw_title     = Column(Unicode(STR_MAX))
    nrm_title     = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    title_prefix  = Column(Unicode(STR_MAX))
    raw_company   = Column(Unicode(STR_MAX))
    nrm_company   = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    description   = Column(Unicode(STR_MAX))
    text_length   = Column(Integer)
    n_experiences = Column(Integer)
    first_experience_start = Column(DateTime)
    last_experience_start  = Column(DateTime)
    nrm_first_title = Column(Unicode(STR_MAX),
                             ForeignKey('entity.nrm_name'),
                             index=True)
    first_title_prefix = Column(Unicode(STR_MAX))
    nrm_first_company = Column(Unicode(STR_MAX),
                               ForeignKey('entity.nrm_name'),
                               index=True)
    nrm_curr_title = Column(Unicode(STR_MAX),
                            ForeignKey('entity.nrm_name'),
                            index=True)
    curr_title_prefix = Column(Unicode(STR_MAX))
    n_educations  = Column(Integer)
    first_education_start  = Column(DateTime)
    last_education_start   = Column(DateTime)
    last_education_end     = Column(DateTime)
    nrm_last_institute = Column(Unicode(STR_MAX),
                                ForeignKey('entity.nrm_name'),
                                index=True)
    nrm_last_subject = Column(Unicode(STR_MAX),
                              ForeignKey('entity.nrm_name'),
                              index=True)
    nrm_last_degree = Column(Unicode(STR_MAX),
                             ForeignKey('entity.nrm_name'),
                             index=True)
    url           = Column(String(STR_MAX))
    indexed_on    = Column(DateTime, index=True)
    crawled_on    = Column(DateTime, index=True)

    title = relationship('Entity',
                         primaryjoin='INProfile.nrm_title==Entity.nrm_name')
    company = relationship('Entity',
                           primaryjoin='INProfile.nrm_company==Entity.nrm_name')
    location = relationship('Location')
    skills = relationship('INProfileSkill',
                          order_by='INProfileSkill.nrm_name',
                          cascade='all, delete-orphan')
    experiences = relationship('INExperience',
                               order_by='INExperience.start',
                               cascade='all, delete-orphan')
    educations = relationship('INEducation',
                              order_by='INEducation.start',
                              cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('datoin_id'),)

class INExperience(SQLBase):
    __tablename__ = 'inexperience'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX))
    inprofile_id  = Column(BigInteger,
                           ForeignKey('inprofile.id'),
                           index=True)
    language      = Column(String(20))
    raw_title     = Column(Unicode(STR_MAX))
    nrm_title     = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    title_prefix  = Column(Unicode(STR_MAX))
    raw_company   = Column(Unicode(STR_MAX))
    nrm_company   = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    place_id      = Column(String(STR_MAX), ForeignKey('location.place_id'))
    start         = Column(DateTime)
    end           = Column(DateTime)
    duration      = Column(Integer)
    description   = Column(Unicode(STR_MAX))
    indexed_on    = Column(DateTime)

    title = relationship(
        'Entity', primaryjoin='INExperience.nrm_title==Entity.nrm_name')
    company = relationship(
        'Entity', primaryjoin ='INExperience.nrm_company==Entity.nrm_name')
    skills = relationship('INExperienceSkill',
                          order_by='INExperienceSkill.nrm_skill',
                          cascade='all, delete-orphan')

class INEducation(SQLBase):
    __tablename__ = 'ineducation'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX))
    inprofile_id  = Column(BigInteger,
                           ForeignKey('inprofile.id'),
                           index=True)
    language      = Column(String(20))
    raw_institute = Column(Unicode(STR_MAX))
    nrm_institute = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    rawdegree     = Column(Unicode(STR_MAX))
    nrm_degree    = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    rawsubject    = Column(Unicode(STR_MAX))
    nrm_subject   = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True)
    start         = Column(DateTime)
    end           = Column(DateTime)
    description   = Column(Unicode(STR_MAX))
    indexed_on    = Column(DateTime)

    institute = relationship(
        'Entity', primaryjoin='INEducation.nrm_institute==Entity.nrm_name')
    degree = relationship(
        'Entity', primaryjoin='INEducation.nrm_degree==Entity.nrm_name')
    subject = relationship(
        'Entity', primaryjoin='INEducation.nrm_subject==Entity.nrm_name')

class INProfileSkill(SQLBase):
    __tablename__ = 'inprofile_skill'
    inprofile_id  = Column(BigInteger,
                           ForeignKey('inprofile.id'),
                           primary_key=True,
                           index=True,
                           autoincrement=False)
    nrm_name      = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           primary_key=True,
                           index=True,
                           autoincrement=False)
    reenforced    = Column(Boolean)

    skill = relationship('Entity')

class INExperienceSkill(SQLBase):
    __tablename__ = 'inexperience_skill'
    inexperience_id = Column(BigInteger,
                             ForeignKey('inexperience.id'),
                             primary_key=True,
                             index=True,
                             autoincrement=False)
    nrm_skill     = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           primary_key=True,
                           index=True,
                           autoincrement=False)

    skill = relationship('Entity')


# entities

class Entity(SQLBase):
    __tablename__ = 'entity'
    nrm_name      = Column(Unicode(STR_MAX),
                           primary_key=True,
                           autoincrement=False)
    type          = Column(String(20), index=True)
    source        = Column(String(20), index=True)
    language      = Column(String(20), index=True)
    name          = Column(Unicode(STR_MAX))
    profile_count = Column(BigInteger, index=True)
    sub_document_count = Column(BigInteger, index=True)

class Word(SQLBase):
    __tablename__ = 'word'
    word          = Column(Unicode(STR_MAX),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    nrm_name      = Column(Unicode(STR_MAX),
                           ForeignKey('entity.nrm_name'),
                           index=True,
                           primary_key=True,
                           autoincrement=False)
    type          = Column(String(20), index=True)
    source        = Column(String(20), index=True)
    language      = Column(String(20), index=True)


class EntityEntity(SQLBase):
    __tablename__ = 'entity_entity'
    nrm_name1     = Column(Unicode(STR_MAX),
                           primary_key=True,
                           autoincrement=False)
    nrm_name2     = Column(Unicode(STR_MAX),
                           primary_key=True,
                           autoincrement=False)
    source        = Column(String(20), index=True)
    language      = Column(String(20), index=True)
    type1         = Column(String(20), index=True)
    type2         = Column(String(20), index=True)
    profile_count = Column(BigInteger)
    sub_document_count = Column(BigInteger)


# locations

class Location(SQLBase):
    __tablename__ = 'location'
    place_id      = Column(String(STR_MAX),
                           primary_key=True,
                           autoincrement=False)
    name          = Column(Unicode(STR_MAX))
    geo           = Column(Geometry('POINT'))

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
    postcode_id   = Column(BigInteger,
                           ForeignKey('postcode.id'),
                           primary_key=True,
                           index=True,
                           autoincrement=False)
    type          = Column(String(20))
    country       = Column(Unicode(STR_MAX))


def skill_score(coincidence_count, category_count, skill_count, nrecords):
    """Measure how strongly a skill is associated with a certain category.

    Args:
      coincidence_count (int): Number of times the skill appears in records of
        the desired category.
      category_count (int): The total number of records belonging to the
        category.
      skill_count (int): The total number of records associated with the skill.
      nrecords (int): The total number of records.

    Returns:
      float: A number between -1 and 1 measuring the strength of the
        relationship between the skill and the category. A value of 1 indicates
        a strong relationship, 0 means no relation, and -1 means that the skill
        and the category are mutually exclusive.

    """
    return coincidence_count/category_count \
        - (skill_count-coincidence_count)/(nrecords-category_count)


class AnalyticsDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def add_liprofile(self, liprofile):
        """Add a LinkedIn profile to the database.

        Args:
          liprofile (dict): A ``dict`` describing the LinkedIn profile. Valid
            fields are:

              * all columns of LIProfile *
              skills (list of dict)
                nrm_name
              experiences (list of dict)
                * all columns of LIExperience *
                skills (list of str)
                  nrm_name
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
                if not skill or not skill.get('nrm_name', None):
                    continue
                skill.pop('liprofile_id', None)
                skill.pop('skill', None)
                if skill['nrm_name'] not in skillnames:
                    skillnames.add(skill['nrm_name'])
                    newskills.append(skill)
            liprofile['skills'] = newskills

        if liprofile.get('experiences', None) is not None:
            for liexperience in liprofile['experiences']:
                liexperience.pop('id', None)
                liexperience.pop('liprofile_id', None)
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
                            newskills.append({'nrm_skill' : skill})
                            skillnames.add(skill)
                    liexperience['skills'] = newskills

        if liprofile.get('educations', None) is not None:
            for lieducation in liprofile['educations']:
                lieducation.pop('id', None)
                lieducation.pop('institute', None)
                lieducation.pop('degree', None)
                lieducation.pop('subject', None)
                lieducation['language'] = language

        return self.add_from_dict(liprofile, LIProfile)

    def add_inprofile(self, inprofile):
        """Add a LinkedIn profile to the database.

        Args:
          inprofile (dict): A ``dict`` describing the LinkedIn profile. It must
            contain the following fields:

              * all columns of INProfile *
              skills (list of dict)
                nrm_name
              experiences (list of dict)
                * all columns of INExperience *
                skills (list of str)
                  nrm_name
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
                if not skill or not skill.get('nrm_name', None):
                    continue
                skill.pop('inprofile_id', None)
                skill.pop('skill', None)
                if skill['nrm_name'] not in skillnames:
                    skillnames.add(skill['nrm_name'])
                    newskills.append(skill)
            inprofile['skills'] = newskills

        if inprofile.get('experiences', None) is not None:
            for inexperience in inprofile['experiences']:
                inexperience.pop('id', None)
                inexperience.pop('inprofile_id', None)
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
                            newskills.append({'nrm_skill' : skill})
                            skillnames.add(skill)
                    inexperience['skills'] = newskills

        if inprofile.get('educations', None) is not None:
            for ineducation in inprofile['educations']:
                ineducation.pop('id', None)
                ineducation.pop('institute', None)
                ineducation.pop('degree', None)
                ineducation.pop('subject', None)
                ineducation['language'] = language

        return self.add_from_dict(inprofile, INProfile)

    def find_entities(self, querytype, source, language, querytext,
                     min_profile_count=None, min_sub_document_count=None,
                     exact=False):
        if querytype == 'title':
            nrmfunc = normalized_title
        elif querytype == 'skill':
            nrmfunc = normalized_skill
        elif querytype == 'company':
            nrmfunc = normalized_company
        elif querytype == 'sector':
            nrmfunc = lambda src, lang, name: normalized_sector(name)
        elif querytype == 'institute':
            nrmfunc = normalized_institute
        elif querytype == 'subject':
            nrmfunc = normalized_subject
        elif querytype == 'degree':
            nrmfunc = normalized_degree
        else:
            raise ValueError('Unsupported query type `{0:s}`.' \
                             .format(querytype))

        if exact:
            entitynames = [nrmfunc(source, language, querytext)]
        else:
            words = split_nrm_name(nrmfunc(source, language, querytext))[-1] \
                    .split()
            words = list(set(words))
            if not words:
                return [], []

            q = self.query(Word.nrm_name)
            filters = [Word.type == querytype,
                       Word.source == source,
                       Word.language == language,
                       Word.word == words[0]]
            for word in words[1:]:
                word_alias = aliased(Word)
                q = q.join(word_alias, word_alias.nrm_name == Word.nrm_name)
                filters.append(word_alias.word == word)
            q = q.filter(*filters).distinct()
            entitynames = [entity for entity, in q]

        entities = []
        if entitynames:
            q = self.query(Entity.nrm_name,
                           Entity.name,
                           Entity.profile_count,
                           Entity.sub_document_count) \
                    .filter(Entity.nrm_name.in_(entitynames))
            if min_profile_count is not None:
                q = q.filter(Entity.profile_count >= min_profile_count)
            if min_sub_document_count is not None and querytype != 'sector':
                q = q.filter(Entity.sub_document_count \
                             >= min_sub_document_count)
            for nrm_name, name, profile_count, sub_document_count in q:
                if not profile_count:
                    profile_count = 0
                if not sub_document_count:
                    sub_document_count = 0
                entities.append((nrm_name, name, profile_count,
                                 sub_document_count))

        return entities


