import os

# Datoin Database 
DATOIN_DB     = 'postgresql://geektalent:geektalent@localhost/datoin'

# Canonical Database
CANONICAL_DB  = 'postgresql://geektalent:geektalent@localhost/canonical'

# Analytics Database
ANALYTICS_DB  = 'postgresql://geektalent:geektalent@localhost/analytics'

# GeekMaps Database
GEEKMAPS_DB   = 'postgresql://geektalent:geektalent@localhost/geekmaps'

# Career Definition Database
CAREERDEFINITION_DB \
    = 'postgresql://geektalent:geektalent@localhost/careerdefinition'


# search endpoint from DATOIN
DATOIN_SEARCH   = 'http://gt1.datoin.com:8765/search'
# profiles endpoint from DATOIN
DATOIN_PROFILES = 'http://gt1.datoin.com:8765/profiles'

# search endpoint for DATOIN returning one document per profile
DATOIN2_SEARCH   = 'http://gt.datoin.com:8567/search'
# search endpoint for DATOIN API v3
DATOIN3_SEARCH   = 'http://api.datoin.com:8765/search'

# URL for Google Geocoding API
GEOCODING_API = 'https://maps.googleapis.com/maps/api/geocode/json'

# URL for Google Places API
PLACES_API = 'https://maps.googleapis.com/maps/api/place/textsearch/json'

# Key for Google Places API
# Alek's key: AIzaSyBipNiucZXVvVUmOrrV5voUhcsuoyiTPvI
# Datascience key: AIzaSyA9rjoaF9BHgl7sC9FrQxlz8DjTjIVVfcI
PLACES_KEY = 'AIzaSyA9rjoaF9BHgl7sC9FrQxlz8DjTjIVVfcI'

# Watson concept insights graph url
WATSON_CONCEPT_INSIGHTS_GRAPH_URL \
    = 'https://gateway.watsonplatform.net/concept-insights/api/v2/graphs' \
      '/wikipedia/en-latest/'
# Watson username
WATSON_USERNAME = '8d4b1f2e-7878-45be-8eaa-4207e29df6e7'
# Watson password
WATSON_PASSWORD = 'SLtaOke1oa2p'

# maximum number of failed crawls before profile gets deleted
MAX_CRAWL_FAIL_COUNT = 3

# minimum return value of phraseMatch for which constitutes match
SKILL_MATCHING_THRESHOLD = 0.75

# source directory
_srcdir = os.path.dirname(os.path.abspath(__file__))

# Absolute path to NUTS data
NUTS_DATA = os.path.join(_srcdir,
                         'NUTS_2013_SHP', 'data', 'NUTS_RG_01M_2013.shp')

# Absolute path to UK postcodes
POSTCODES_UK = os.path.join(_srcdir, 'postcodes', 'ukpostcodes.csv')

# Absolute path to NL postcodes
POSTCODES_NL = os.path.join(_srcdir, 'postcodes', 'postcode_NL_en.csv')
