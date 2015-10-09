__all__ = [
    'LIProfile',
    'Experience',
    'Education',
    'DatoinDB',
    ]

from sqldb import *
from sqlalchemy import \
    Column, \
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
from phrasematch import clean, stem, tokenize, matchStems


STR_MAX = 100000

SQLBase = sqlbase()

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    name              = Column(Unicode(STR_MAX))
    location          = Column(Unicode(STR_MAX))
    nrmLocation       = Column(Unicode(STR_MAX))
    title             = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    nrmDescription    = Column(Unicode(STR_MAX))
    totalExperience   = Column(Integer)
    profileUrl        = Column(String(STR_MAX))
    profilePictureUrl = Column(String(STR_MAX))
    indexedOn         = Column(Date, index=True)

    __table_args__ = (UniqueConstraint('datoinId'),)

class Experience(SQLBase):
    __tablename__ = 'experience'
    id             = Column(BigInteger, primary_key=True)
    datoinId       = Column(String(STR_MAX))
    profileId      = Column(BigInteger,
                            ForeignKey('liprofile.id'),
                            index=True)
    title          = Column(Unicode(STR_MAX))
    nrmTitle       = Column(Unicode(STR_MAX))
    company        = Column(Unicode(STR_MAX))
    nrmCompany     = Column(Unicode(STR_MAX))
    start          = Column(Date)
    end            = Column(BigInteger)
    duration       = Column(Integer)
    description    = Column(Unicode(STR_MAX))
    nrmDescription = Column(Unicode(STR_MAX))
    indexedOn      = Column(Date)

class Education(SQLBase):
    __tablename__ = 'education'
    id          = Column(Integer, primary_key=True)
    datoinId    = Column(String(STR_MAX))
    profileId   = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         index=True)
    institute      = Column(Unicode(STR_MAX))
    nrmInstitute   = Column(Unicode(STR_MAX))
    degree         = Column(Unicode(STR_MAX))
    nrmDegree      = Column(Unicode(STR_MAX))
    subject        = Column(Unicode(STR_MAX))
    nrmSubject     = Column(Unicode(STR_MAX))
    start          = Column(Date)
    end            = Column(Date)
    description    = Column(Unicode(STR_MAX))
    nrmDescription = Column(Unicode(STR_MAX))
    indexedOn      = Column(Date)

class Skill(SQLBase):
    __tablename__ = 'skill'
    id        = Column(BigInteger, primary_key=True)
    profileId = Column(BigInteger, ForeignKey('liprofile.id'))
    name      = Column(Unicode(STR_MAX))
    nrmName   = Column(Unicode(STR_MAX))

class ExperienceSkill(SQLBase):
    __tablename__ = 'skill'
    experienceId = Column(BigInteger, ForeignKey('experience.id'),
                          primary_key=True)
    skillId      = Column(BigInteger, ForeignKey('skill.id'),
                          primary_key=True)


def normalizedSkill(name):
    if not name:
        return None
    nname = stem(name)
    if not nname:
        return None
    nname.sort()
    return ' '.join(nname)

def normalizedTitle(name):
    if not name:
        return None
    name = clean(name, keep='&/-', removebrackets=True)
    name = name.split(' - ')[0]
    name = name.split(' at ')[0]
    name = name.split(',')[0]
    nname = stem(name, removebrackets=True)
    if not nname:
        return None
    return ' '.join(nname)

def normalizedCompany(name):
    if not name:
        return None
    nname = tokenize(name, removebrackets=True)
    if not nname:
        return None
    return ' '.join(nname)

def normalizedLocation(name):
    return ' '.join(name.lower().split())

def _getStartDate(experience):
    startdate = experience.get('startdate', DATE0)
    if startdate is None:
        return DATE0
    return startdate

    
class NormalFormDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addExperience(self, profileId, edict, now):        
        experience = Experience()
        experience.datoinId       = edict['datoinId']
        experience.profileId      = profileId
        experience.title          = edict['title']
        experience.nrmTitle       = normalizedTitle(edict['title'])
        experience.company        = edict['company']
        experience.nrmCompany     = normalizedCompany(edict['company'])
        experience.start          = edict['start']
        experience.end            = edict['end']
        experience.description    = edict['description']
        experience.indexedOn      = edict['indexedOn']

        # work out duration
        duration = None        
        if experience.start is not None and experience.end is not None:
            if experience.start < experience.end:
                duration = (enddate - startdate).days
        elif experience.start is not None:
            duration = (now - startdate).days
        experience.duration = duration
        
        self.add(experience)
        return experience

    def addEducation(self, profileId, edict):
        education = Education()
        education.datoinId       = edict['datoinId']
        education.profileId      = profileId
        education.institute      = edict['institute']
        education.nrmInstitute   = normalizedInstitute(edict['institute'])
        education.degree         = edict['degree']
        education.nrmDegree      = normalizedDegree(edict['degree'])
        education.subject        = edict['subject']
        education.nrmSubject     = normalizedSubject(edict['subject'])
        education.start          = edict['start']
        education.end            = edict['end']
        education.description    = edict['description']
        education.indexedOn      = edict['indexedOn']
        self.add(education)
        return education

    def addSkill(self, profileId, skillname):
        skill = Skill()
        skill.profileId = profileId
        skill.name      = skillname
        skill.nrmName   = normalizedSkill(skillname)
        skill.rank      = 0.0
        self.add(skill)
        return skill

    def rankSkills(self, skills, experiences, liprofile):
        descriptionstems = [stem(experience.description) \
                            for experience in experiences]
        skillstems = [skill.nrmName.split() for skill in skills]

        # match experience descriptions
        matches = (matchStems(skillstems, descriptionstems,
                              threshold=conf.SKILL_MATCHING_THRESHOLD) > \
                   conf.SKILL_MATCHING_THRESHOLD)
        ranks = np.zeros(len(skills))
        for iexperience, experience in enumerate(experiences):
            for iskill, skill in enumerate(skills):
                if matches[iskill, iexperience]:
                    if experience.duration:
                        duration = experience.duration
                    else:
                        duration = 0
                    ranks[iskill] += duration/365.0
                    self.add(ExperienceSkill(experienceId=experience.id,
                                             skillId=skill.id))

        # match profile text
        profiletext = ''
        if liprofile.title is not None:
            profiletext += liprofile.title
        if liprofile.description is not None:
            profiletext += ' '+liprofile.description
        matches = (matchStems(skillstems, [stem(profiletext)],
                              threshold=conf.SKILL_MATCHING_THRESHOLD) > \
                   conf.SKILL_MATCHING_THRESHOLD)
        for iskill, skill in enumerate(skills):
            if matches[iskill, 0]:
                # half of the total work experience for skills mentioned in
                # the profile summary
                ranks[iskill] += liprofile.totalExperience/365.0/2.0

        # update skill ranks
        for iskill, skill in enumerate(skills):
            skill.rank = ranks[iskill]
        
    
    def addLIProfile(self, profile, experiencedicts, educationdicts):
        # create or update LIProfile
        liprofile = self.query(LIProfile) \
                        .filter(LIProfile.datoinId == profile['datoinId']) \
                        .first()
        if not liprofile:
            isnew = True
            liprofile = LIProfile()
        else:
            isnew = False

        liprofile.datoinId        = profile['datoinId']
        liprofile.name            = profile['name']
        liprofile.nrmLocation     = normalizedLocation(profile['location'])
        liprofile.title           = profile['title']
        liprofile.nrmTitle        = normalizedTitle(profile['title'])
        liprofile.description     = profile['description']
        liprofile.nrmDescription  = normalizedDescription(profile['description'])
        liprofile.totalexperience = 0
        liprofile.url             = profile['url']
        liprofile.pictureUrl      = profile['pictureUrl']
        liprofile.indexedOn       = profile['indexedOn']

        if isnew:
            self.add(liprofile)
            self.flush()

        # add experiences
        if not isnew:
            self.query(Experience) \
                .filter(Experience.profileId == liprofile.id) \
                .delete(synchronize_session='fetch')
        experiences = [self.addExperience(liprofile.id, e) \
                       for e in experiencedicts]
        liprofile.totalExperience = sum([e.duration for e in experiences \
                                         if e.duration is not None])

        # add educations
        if not isnew:
            self.query(Education) \
                .filter(Education.profileId == liprofile.id) \
                .delete(synchronize_session='fetch')
        for edict in educationdicts:
            self.addEducation(liprofile.id, edict)

        # add skills
        if not isnew:
            self.query(Skill) \
                .filter(Skill.profileId == liprofile.id) \
                .delete(synchronize_session='fetch')
        skills = [self.addSkill(liprofile.id, skill) \
                  for skill in profile['skills']]

        # flush session
        self.flush()

        # rank skills and fill ExperienceSkill
        self.rankSkills(skills, experiences, liprofile)
        
        return liprofile
