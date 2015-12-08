__all__ = [
    'Skill',
    'SkillWord',
    'Title',
    'TitleWord',
    'Company',
    'CompanyWord',
    'Word',
    'LIProfileSkill',
    'GeekMapsDB',
    ]

import conf
import numpy as np
import requests
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
    Float, \
    func, \
    or_
from geoalchemy2 import Geometry


STR_MAX = 100000

SQLBase = sqlbase()


class Skill(SQLBase):
    __tablename__ = 'skill'
    nrmName         = Column(Unicode(STR_MAX),
                             primary_key=True,
                             autoincrement=False)
    language        = Column(String(20))
    name            = Column(Unicode(STR_MAX))
    liprofileCount  = Column(BigInteger)
    experienceCount = Column(BigInteger)

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
    liprofileCount  = Column(BigInteger)
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
    count     = Column(BigInteger)

class Company(SQLBase):
    __tablename__ = 'company'
    nrmName   = Column(Unicode(STR_MAX),
                       primary_key=True,
                       autoincrement=False)
    language  = Column(String(20))
    name      = Column(Unicode(STR_MAX))
    liprofileCount  = Column(BigInteger)
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

class Word(SQLBase):
    __tablename__ = 'word'
    language               = Column(String(20),
                                    primary_key=True)
    word                   = Column(Unicode(STR_MAX),
                                    primary_key=True)
    liprofileSkillCount    = Column(BigInteger)
    experienceSkillCount   = Column(BigInteger)
    liprofileTitleCount    = Column(BigInteger)
    experienceTitleCount   = Column(BigInteger)
    liprofileCompanyCount  = Column(BigInteger)
    experienceCompanyCount = Column(BigInteger)
    
class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    id                = Column(BigInteger, primary_key=True)
    profileId         = Column(BigInteger, index=True)
    language          = Column(String(20))
    nrmSkill          = Column(Unicode(STR_MAX),
                               ForeignKey('skill.nrmName'),
                               index=True)
    location          = Column(Unicode(STR_MAX))
    nuts0             = Column(String(2), index=True)
    nuts1             = Column(String(3), index=True)
    nuts2             = Column(String(4), index=True)
    nuts3             = Column(String(5), index=True)
    nrmTitle          = Column(Unicode(STR_MAX),
                               ForeignKey('title.nrmName'),
                               index=True)
    nrmCompany        = Column(Unicode(STR_MAX),
                               ForeignKey('company.nrmName'),
                               index=True)
    rank              = Column(Float)


    
class GeekMapsDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addLIProfileSkill(self, profileId, language, location, nutsid,
                          nrmTitle, nrmCompany, nrmSkill,
                          rank):
        if not nutsid or \
           location in ['United Kingdom', 'Netherlands', 'Nederland']:
            return LIProfileSkill()
        
        liprofileskill \
            = self.query(LIProfileSkill) \
                  .filter(LIProfileSkill.profileId == profileId,
                          LIProfileSkill.nrmSkill == nrmSkill) \
                  .first()
        if not liprofileskill:
            liprofileskill = LIProfileSkill(profileId=profileId,
                                            nrmSkill=nrmSkill)
            self.add(liprofileskill)
        if nutsid is None:
            nuts = [None]*4
        else:
            nuts = [nutsid[:i] for i in range(2, 6)]
        liprofileskill.language    = language
        liprofileskill.location    = location
        liprofileskill.nuts0       = nuts[0]
        liprofileskill.nuts1       = nuts[1]
        liprofileskill.nuts2       = nuts[2]
        liprofileskill.nuts3       = nuts[3]
        liprofileskill.nrmTitle    = nrmTitle
        liprofileskill.nrmCompany  = nrmCompany
        liprofileskill.rank        = rank

        return liprofileskill

    def findEntities(self, querytype, language, querytext, exact=False):
        if querytype == 'title':
            wordtable = TitleWord
            wordcountcols = [Word.liprofileTitleCount]
            entitytable = Title
            entitycountcols = [Title.liprofileCount]
            nrmfunc = normalizedTitle
        elif querytype == 'skill':
            wordtable = SkillWord
            wordcountcols = [Word.experienceSkillCount,
                             Word.liprofileSkillCount]
            entitytable = Skill
            entitycountcols = [Skill.experienceCount, Skill.liprofileCount]
            nrmfunc = normalizedSkill
        elif querytype == 'company':
            wordtable = CompanyWord
            wordcountcols = [Word.liprofileSkillCount]
            entitytable = Company
            entitycountcols = [Company.liprofileCount]
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
        wordcounts.sort(key=lambda x: x[-1])
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
