import conf
from analyticsdb import *
from windowquery import processDb
from logger import Logger
import sys
import csv


andb = AnalyticsDB(conf.ANALYTICS_DB)
logger = Logger(sys.stdout)

# clear postcode table
andb.query(Postcode).delete()
andb.commit()

# read UK postcodes

logger.log('Processing UK postcodes.\n')
with open(conf.POSTCODES_UK, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    header = next(csvreader)
    postcodeCol  = header.index('postcode')
    latitudeCol  = header.index('latitude')
    longitudeCol = header.index('longitude')
    townCol      = header.index('town')
    regionCol    = header.index('region')
    stateCol     = header.index('country_string')

    def addPostcode(row):
        country = 'United Kingdom'
        state    = row[stateCol] if row[stateCol] else None
        region   = row[regionCol] if row[regionCol] else None
        town     = row[townCol] if row[townCol] else None
        postcode = row[postcodeCol]
        lat      = float(row[latitudeCol])
        lon      = float(row[longitudeCol])
        geo      = 'POINT({0:f} {1:f})'.format(lon, lat)
        
        postcodeObj = Postcode(country=country,
                               state=state,
                               region=region,
                               town=town,
                               postcode=postcode,
                               geo=geo)
        andb.add(postcodeObj)

    processDb(csvreader, addPostcode, andb, logger=logger)

logger.log('\nProcessing NL postcodes.\n')
with open(conf.POSTCODES_NL, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    header = next(csvreader)
    postcodeCol  = header.index('postcode')
    latitudeCol  = header.index('lat')
    longitudeCol = header.index('lon')
    townCol      = header.index('city')
    regionCol    = header.index('municipality')
    stateCol     = header.index('province')

    def addPostcode(row):
        country = 'Netherlands'
        state    = row[stateCol] if row[stateCol] else None
        region   = row[regionCol] if row[regionCol] else None
        town     = row[townCol] if row[townCol] else None
        postcode = row[postcodeCol]
        lat      = float(row[latitudeCol])
        lon      = float(row[longitudeCol])
        geo      = 'POINT({0:f} {1:f})'.format(lon, lat)
        
        postcodeObj = Postcode(country=country,
                               state=state,
                               region=region,
                               town=town,
                               postcode=postcode,
                               geo=geo)
        andb.add(postcodeObj)

    processDb(csvreader, addPostcode, andb, logger=logger)
    


