import conf
from geektalentdb import *

gtdb = GeekTalentDB(url=conf.WRITE_DB_URL)

gtdb.add_location('London, UK')

# country = 'United Kingdom'
# statefiles = {'England' : 'england.dat'}
    

# locations = []
# for state, filename in statefiles.items():
#     with open(filename, 'r') as datafile:
#         for line in datafile:
#             fields = line.split('\t')
#             region = fields[1].strip()
#             location = fields[0].strip()            
#             locations.append((country, state, region, location))

# locations.sort()
# for country, state, region, location in locations:
#     gtdb.add_location(location, region, state, country)

gtdb.commit()

