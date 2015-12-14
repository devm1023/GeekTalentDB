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

# Canonical Database
CANONICAL_DB  = 'postgresql://geektalent:Ta2tqaltuaatri42@localhost/canonical'

# Analytics Database
ANALYTICS_DB  = 'postgresql://geektalent:Ta2tqaltuaatri42@localhost/analytics'

# GeekMaps Database
GEEKMAPS_DB   = 'postgresql://geektalent:Ta2tqaltuaatri42@localhost/geekmaps'


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

# URL for Google Geocoding API
GEOCODING_API = 'https://maps.googleapis.com/maps/api/geocode/json'

# URL for Google Places API
PLACES_API = 'https://maps.googleapis.com/maps/api/place/textsearch/json'

# Key for Google Places API
# Alek's key: AIzaSyBipNiucZXVvVUmOrrV5voUhcsuoyiTPvI
# Datascience key: AIzaSyDH8TObTWteqBAL7EfvgwRFFVUqhDUSLOM
PLACES_KEY = 'AIzaSyDH8TObTWteqBAL7EfvgwRFFVUqhDUSLOM'

# maximum number of profiles which get added to the DB
MAX_PROFILES    = None

# minimum return value of phraseMatch for which constitutes match
SKILL_MATCHING_THRESHOLD = 0.75

# Number of attempts at downloading corrupt profiles
MAX_ATTEMPTS = 2

# Absolute path to NUTS data
NUTS_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'NUTS_2013_SHP', 'data', 'NUTS_RG_01M_2013.shp')
