__all__ = [
    'LIProfile',
    'TaxonomyDB',
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
    Boolean, \
    func
from sqlalchemy.dialects.postgresql import ARRAY as Array
from ltree import LTREE as LTree
import numpy as np


STR_MAX = 100000

SQLBase = sqlbase()

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(String(STR_MAX), primary_key=True)
    parentId          = Column(String(STR_MAX))
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
    name              = Column(Unicode(STR_MAX))
    country           = Column(Unicode(STR_MAX))
    city              = Column(Unicode(STR_MAX))
    sector            = Column(Unicode(STR_MAX))
    title             = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    profileUrl        = Column(String(STR_MAX))
    profilePictureUrl = Column(String(STR_MAX))
    indexedOn         = Column(BigInteger, index=True)
    crawledDate       = Column(BigInteger, index=True)
    connections       = Column(String(STR_MAX))
    categories        = Column(Array(Unicode(STR_MAX)))
    groups            = Column(Array(Unicode(STR_MAX)))
    isCompany         = Column(Boolean)
    recordid          = Column(Integer)
    hasusefultext     = Column(Boolean)
    ibm_tax1          = Column(LTree)
    ibm_tax1_cfscore  = Column(Float)
    ibm_tax1_cf       = Column(Boolean)
    ibm_tax2          = Column(LTree)
    ibm_tax2_cfscore  = Column(Float)
    ibm_tax2_cf       = Column(Boolean)
    ibm_tax3          = Column(LTree)
    ibm_tax3_cfscore  = Column(Float)
    ibm_tax3_cf       = Column(Boolean)

def _taxonomyFromLTree(ltree):
    return ltree.replace('_', ' ').replace('.', '/')

def _ltreeFromTaxonomy(tax):
    return tax.replace(' ', '_').replace('/', '.')
    
class TaxonomyDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def getTaxonomies(self, ids, taxonomyNames):
        q = self.query(LIProfile.id,
                       LIProfile.ibm_tax1,
                       LIProfile.ibm_tax1_cfscore,
                       LIProfile.ibm_tax1_cf,
                       LIProfile.ibm_tax2,
                       LIProfile.ibm_tax2_cfscore,
                       LIProfile.ibm_tax2_cf,
                       LIProfile.ibm_tax3,
                       LIProfile.ibm_tax3_cfscore,
                       LIProfile.ibm_tax3_cf) \
                .filter(LIProfile.id.in_(ids))
        iddict = dict((id, []) for id in ids)
        for id, tax1, score1, cf1, tax2, score2, cf2, tax3, score3, cf3 in q:
            if tax1 is not None and score1 is not None and cf1 is not None:
                iddict[id].append({'name'        : taxonomyNames[tax1],
                                   'score'       : score1,
                                   'isConfident' : cf1})
            if tax2 is not None and score2 is not None and cf2 is not None:
                iddict[id].append({'name'        : taxonomyNames[tax2],
                                   'score'       : score2,
                                   'isConfident' : cf2})
            if tax3 is not None and score3 is not None and cf3 is not None:
                iddict[id].append({'name'        : taxonomyNames[tax3],
                                   'score'       : score3,
                                   'isConfident' : cf3})
                
        return [{'id' : id, 'taxonomies' : taxonomies} \
                for id, taxonomies in iddict.items()]

    def getUsers(self, taxonomies, nusers, randomize, confidence, taxonomyIds):
        taxonomies = list(set(taxonomies))
        limit = nusers if not randomize else 1000
        ltrees = [taxonomyIds[t] for t in taxonomies]
        results = []
        for ltree, taxonomy in zip(ltrees, taxonomies):
            result = {'taxonomy' : taxonomy, 'ids' : []}
            q1 = self.query(LIProfile.id, LIProfile.ibm_tax1_cfscore) \
                     .filter(LIProfile.ibm_tax1.descendant_of(ltree),
                             LIProfile.ibm_tax1_cfscore >= confidence) \
                     .order_by(LIProfile.ibm_tax1_cfscore) \
                     .limit(limit)
            q2 = self.query(LIProfile.id, LIProfile.ibm_tax2_cfscore) \
                     .filter(LIProfile.ibm_tax2.descendant_of(ltree),
                             LIProfile.ibm_tax2_cfscore >= confidence) \
                     .order_by(LIProfile.ibm_tax2_cfscore) \
                     .limit(limit)
            q3 = self.query(LIProfile.id, LIProfile.ibm_tax3_cfscore) \
                     .filter(LIProfile.ibm_tax3.descendant_of(ltree),
                             LIProfile.ibm_tax3_cfscore >= confidence) \
                     .order_by(LIProfile.ibm_tax3_cfscore) \
                     .limit(limit)

            scores = {}
            for q in [q1, q2, q3]:
                for id, score in q:
                    scores[id] = max(scores.get(id, 0.0), score)
            scores = list(scores.items())
            scores.sort(key=lambda x: -x[1])
            
            if len(scores) > limit:
                scores = scores[:limit]
            ids = [id for id, score in scores]
            if randomize and ids:
                ids = np.array(ids)
                size = min(nusers, len(ids))
                ids = list(np.random.choice(ids, (size,), replace=False))

            results.append({'taxonomy' : taxonomy, 'ids' : ids})
        return results
                       

if __name__ == '__main__':
    ids = ['linkedin:profile:35f5c600df114af76710136e4530d5e682d1639a',
           'linkedin:profile:ad3794c89e459ff8da8305c99841b6b8144df3fe',
           'linkedin:profile:20f37f09cad9a84442458ca4b703c1ddd433cc36']
    
    txdb = TaxonomyDB('postgresql://geektalent:Ta2tqaltuaatri42@localhost/datoin2')
    # result = txdb.getTaxonomies(ids)
    # print(result)
    result = txdb.getUsers(['clothes'], 10, True)
    print(result)
