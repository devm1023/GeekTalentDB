import conf
from analyticsdb import *
from sqlalchemy import func, and_, or_, text
from textnormalization import _stopwords, clean, normalizedCompany
from nltk.metrics.distance import edit_distance
import csv
import sys

def substringFilter(col, strings):
    statements = []
    for s in strings:
        statements.append(col == s)
        # statements.append(col.like('% '+s))
        # statements.append(col.like(s+' %'))
        # statements.append(col.like('% '+s+' %'))
    return or_(*statements)

def normalizedNestaCompany(name):
    stopwords = _stopwords | set(['limited', 'ltd', 'uk'])
    return clean(name,
                 nospace='\'’',
                 lowercase=True,
                 removebrackets=True,
                 removestopwords=stopwords)

andb = AnalyticsDB(url=conf.ANALYTICS_DB)

inputfile = sys.argv[1]
outputfile = sys.argv[2]
explicit = False
if len(sys.argv) > 3:
    explicit = sys.argv[3] == '--explicit'

ncompanies = 0
nmatches = 0
with open(inputfile, 'r') as csvinput, \
     open(outputfile, 'w') as csvoutput:
    csvreader = csv.reader(csvinput)
    csvwriter = csv.writer(csvoutput)

    csvwriter.writerow(['registration', 'name', 'matched name', 'profile count'])
    next(csvreader, None)
    for row in csvreader:
        companyId = row[0]
        company = row[1]
        ncompanies += 1

        companies = [company]
        if company.find('.') >= 0:
            companies.append(company.replace('.', ''))
        nrmCompanies = list(map(normalizedNestaCompany, companies))
        
        q = andb.query(Company.nrmName, Company.liprofileCount) \
                .filter(substringFilter(Company.nrmName, nrmCompanies)) \
                .order_by(Company.liprofileCount.desc())

        mindistance = None
        bestrow = None
        nrmCompany = normalizedCompany(company)
        for nrmName, count in q:
            hasmatches = True
            distance = edit_distance(nrmName, nrmCompany)
            if mindistance is None or distance < mindistance:
                mindistance = distance
                bestrow = [companyId, company, nrmName, count]
            if explicit:
                csvwriter.writerow([companyId, company, nrmName, count])
                
        if bestrow is None:
            csvwriter.writerow([companyId, company])
            print(company+': no match')
        else:
            nmatches += 1
            if not explicit:
                csvwriter.writerow(bestrow)
            print(company+': matched')

print('\n{0:3.0f}% of companies matched.'.format(nmatches/ncompanies*100.0))
