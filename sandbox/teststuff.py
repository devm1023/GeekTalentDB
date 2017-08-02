from canonicaldb import *
from sqlalchemy import func
import csv
from phraseextract import PhraseExtractor
from textnormalization import tokenized_skill


# tpl = tuple(i for i in range(5))
# tpl1 = tuple(0 for i in range(1))
# tpl = tpl + tpl1
#
# print(tpl)
#
# a = [0, 1, 2, 3, 4, 5]
#
# print(a[2:])
#
# cndb = CanonicalDB()
#
# nrmcol = LIProfileSkill.nrm_name
# rawcol = LIProfileSkill.name
#
# query = cndb.query(nrmcol, rawcol, func.count()) \
#             .filter(nrmcol != None) \
#             .group_by(nrmcol, rawcol) \
#             .order_by(nrmcol)
#
# def entities(q, countcols=1):
#     currententity = None
#     maxcount = 0
#     totalcounts = tuple(0 for i in range(countcols))
#     bestname = None
#     for row in q:
#         nrm_name = row[0]
#         name = row[1]
#         counts = row[2:]
#         if nrm_name != currententity:
#             if bestname:
#                 yield (currententity, bestname) + totalcounts
#             maxcount = 0
#             totalcounts = tuple(0 for i in range(countcols))
#             bestname = None
#             currententity = nrm_name
#         if counts[0] > maxcount:
#             bestname = name
#             maxcount = counts[0]
#         totalcounts = tuple(t+c for t, c in zip(totalcounts, counts))
#     if bestname:
#         yield (currententity, bestname) + totalcounts
#
#
# for row in entities(query):
#     pass


# text = 'As one of our Customer Advisors you will be responsible for making sure our customers receive the best service we can deliver by putting them at the heart of everything you do. You will be highly motivated, approachable and can demonstrate great skills in building rapport with our customers. You will be required to fulfil and understand our customer’s needs by providing an inspirational, creative consultation and making the customer experience one they will not forget. No day will be the same'
text2 = 'sound Carpetright design As one of our Store Managers, as well as managing your own Carpetright Store, you will be responsible for making sure our customers enjoy a simple, hassle-free shopping experience with us and come back time after time – ensuring we put the customer at the heart of everything we do. You will be required to effectively lead and support the store team in achieving all targets by focusing on the effective management of sales, costs, operations, service, communications and developing colleagues with'


skillfile = 'indeed_skills_trimed.csv'

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

resultlist = list(set(skillextractor(text2)))
print(resultlist)
