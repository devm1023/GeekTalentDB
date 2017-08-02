from canonicaldb import *
from sqlalchemy import func

tpl = tuple(i for i in range(5))
tpl1 = tuple(0 for i in range(1))
tpl = tpl + tpl1

print(tpl)

a = [0, 1, 2, 3, 4, 5]

print(a[2:])

cndb = CanonicalDB()

nrmcol = LIProfileSkill.nrm_name
rawcol = LIProfileSkill.name

query = cndb.query(nrmcol, rawcol, func.count()) \
            .filter(nrmcol != None) \
            .group_by(nrmcol, rawcol) \
            .order_by(nrmcol)

def entities(q, countcols=1):
    currententity = None
    maxcount = 0
    totalcounts = tuple(0 for i in range(countcols))
    bestname = None
    for row in q:
        nrm_name = row[0]
        name = row[1]
        counts = row[2:]
        if nrm_name != currententity:
            if bestname:
                yield (currententity, bestname) + totalcounts
            maxcount = 0
            totalcounts = tuple(0 for i in range(countcols))
            bestname = None
            currententity = nrm_name
        if counts[0] > maxcount:
            bestname = name
            maxcount = counts[0]
        totalcounts = tuple(t+c for t, c in zip(totalcounts, counts))
    if bestname:
        yield (currententity, bestname) + totalcounts


for row in entities(query):
    pass
    print(row)



if skillfile is not None:
    skills = []
    with open(skillfile, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            if row:
                skills.append(row[0])
    tokenize = lambda x: tokenized_skill('en', x)
    skillextractor = PhraseExtractor(skills, tokenize=tokenize)
    del skills
