import os

# SQL DB URLs
#
# Format:
# postgresql://<username>:<password>@<host>/<database>
#
# <username> is always 'geektalent'
# <database> is 'geektalent', 'datoin', 'canonical', or 'geekmaps'
#
# host description   host name/IP      password 
# -----------------------------------------------------
# Martin's Laptop    localhost         Ta2tqaltuaatri42
# internal server    10.140.18.98      geektalent      
# cloud SQL server   173.194.248.106   geektalent      
# cloud PostgreSQL   104.155.42.1      Ta2tqaltuaatri42

# GeekTalent Database
GEEKTALENT_DB = 'postgresql://geektalent:Ta2tqaltuaatri42@localhost/geektalent'

# Datoin Database 
DATOIN_DB     = 'postgresql://geektalent:Ta2tqaltuaatri42@localhost/datoin'

# Datoin Database 
DATOIN2_DB     = 'postgresql://geektalent:Ta2tqaltuaatri42@localhost/datoin2'

# Canonical Database
CANONICAL_DB  = 'postgresql://geektalent:Ta2tqaltuaatri42@localhost/canonical'

# Analytics Database
ANALYTICS_DB  = 'postgresql://geektalent:Ta2tqaltuaatri42@localhost/analytics'

# GeekMaps Database
GEEKMAPS_DB   = 'postgresql://geektalent:Ta2tqaltuaatri42@localhost/geekmaps'

# CareerHacker Database
CAREERHACKER_DB \
    = 'postgresql://geektalent:Ta2tqaltuaatri42@localhost/careerhacker'


# Datoin SOLR endpoints:
# public domains: gt.datoin.com, gt1.datoin.com
# private IP: ???
# port: 8765
# endpoints: search, profiles
# See http://gt.datoin.com:8765/docs/api-specs.html for specs

# search endpoint from DATOIN
DATOIN_SEARCH   = 'http://gt1.datoin.com:8765/search'
# profiles endpoint from DATOIN
DATOIN_PROFILES = 'http://gt1.datoin.com:8765/profiles'

# search endpoint for DATOIN returning one document per profile
DATOIN2_SEARCH   = 'http://gt.datoin.com:8567/search'

# URL for Google Geocoding API
GEOCODING_API = 'https://maps.googleapis.com/maps/api/geocode/json'

# URL for Google Places API
PLACES_API = 'https://maps.googleapis.com/maps/api/place/textsearch/json'

# Key for Google Places API
# Alek's key: AIzaSyBipNiucZXVvVUmOrrV5voUhcsuoyiTPvI
# Datascience key: AIzaSyA9rjoaF9BHgl7sC9FrQxlz8DjTjIVVfcI
PLACES_KEY = 'AIzaSyA9rjoaF9BHgl7sC9FrQxlz8DjTjIVVfcI'

# maximum number of profiles which get added to the DB
MAX_PROFILES    = None

# minimum return value of phraseMatch for which constitutes match
SKILL_MATCHING_THRESHOLD = 0.75

# Number of attempts at downloading corrupt profiles
MAX_ATTEMPTS = 2

# source directory
_srcdir = os.path.dirname(os.path.abspath(__file__))

# Absolute path to NUTS data
NUTS_DATA = os.path.join(_srcdir,
                         'NUTS_2013_SHP', 'data', 'NUTS_RG_01M_2013.shp')

# Absolute path to UK postcodes
POSTCODES_UK = os.path.join(_srcdir, 'postcodes', 'ukpostcodes.csv')

# Absolute path to NL postcodes
POSTCODES_NL = os.path.join(_srcdir, 'postcodes', 'postcode_NL_en.csv')
