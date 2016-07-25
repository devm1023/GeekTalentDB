import os

# Crawl Database
CRAWL_DB      = 'postgresql://geektalent:geektalent@localhost/crawl'

# Parse Database
PARSE_DB      = 'postgresql://geektalent:geektalent@localhost/parse'

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

# Description Database
DESCRIPTION_DB \
    = 'postgresql://geektalent:geektalent@localhost/description'

# Watson Database
WATSON_DB \
    = 'postgresql://geektalent:geektalent@localhost/watson'


# paths for tor browser
#
# Linux:
TOR_BROWSER_BINARY = ('/home/geektalent/.local/opt/tor-browser_en-US/Browser/'
                      'start-tor-browser')
TOR_BROWSER_PROFILE = ('/home/geektalent/.local/opt/tor-browser_en-US/Browser/'
                       'TorBrowser/Data/Browser/profile.default/')
# generate with `tor --hash-password PythonRulez`
TOR_HASHED_PASSWORD = '16:4D3D35770F36F240604F4D73CE11DE2E23D5EC43093B206A21E41F5984'
TOR_PASSWORD = 'PythonRulez'
# Mac OS X:
# TOR_BROWSER_BINARY = '/Applications/TorBrowser.app/Contents/MacOS/firefox'
# TOR_BROWSER_PROFILE = ('/Applications/TorBrowser.app/TorBrowser/Data/Browser/'
#                        'profile.default')


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

# Adzuna access keys
ADZUNA_APP_ID = '5f619739'
ADZUNA_APP_KEY = 'da114b2c3ded37d8cc280d8841e5f7f6'
ADZUNA_HISTOGRAM_API = 'https://api.adzuna.com/v1/api/jobs/gb/histogram'
ADZUNA_HISTORY_API = 'https://api.adzuna.com/v1/api/jobs/gb/history'

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
