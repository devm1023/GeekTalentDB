import conf
from geektalentdb import *

gtdb = GeekTalentDB(url=conf.WRITE_DB_URL)

countries = [Country(id=1, name='United Kingdom')]
states = [
    State(id=1, country_id=1, name='England'),
    State(id=2, country_id=1, name='Northern Ireland'),
    State(id=3, country_id=1, name='Scotland'),
    State(id=4, country_id=1, name='Wales'),
    ]
    

regions = []


for state_id, filename in [
        (1, 'england.dat'),
        ]:
    locations = []
    regioncount = 0
    regionids = {}
    with open(filename, 'r') as datafile:
        for line in datafile:
            fields = line.split('\t')
            region = fields[1].strip()
            location = fields[0].strip()
            if region not in regionids:
                regioncount += 1
                regionids[region] = regioncount
            region_id = regionids[region]

            locations.append(Location(country_id=1,
                                      state_id=state_id,
                                      region_id=region_id,
                                      name=location))
    for region, region_id in regionids.items():
        regions.append(Region(id=region_id, state_id=state_id, country_id=1,
                              name=region))
        
for country in countries:
    print(country.name, country.id)
    gtdb.session.add(country)
gtdb.flush()

for state in states:
    print(state.name, state.id)
    gtdb.session.add(state)
gtdb.flush()

for region in regions: 
    print(region.name, region.id)
    gtdb.session.add(region)
gtdb.flush()

for location in locations:
    gtdb.session.add(location)
gtdb.commit()

