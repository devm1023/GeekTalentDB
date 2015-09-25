# SQL DB URLs
# local db:        'postgresql://geektalent:geektalent@localhost/geektalent'
# internal server: 'postgresql://geektalent:geektalent@10.140.18.98/geektalent'
# cloud SQL:       'mysql+mysqldb://geektalent:geektalent@173.194.248.106/geektalent'
# cloud PostgreSQL 'postgresql://geektalent:Ta2tqaltuaatri42@104.155.42.1/geektalent'

# SQL Database used by scripts which write data
WRITE_DB_URL    = 'postgresql://geektalent:geektalent@localhost/geektalent'

# SQL Database used by scripts which only read data
READ_DB_URL     = 'postgresql://geektalent:Ta2tqaltuaatri42@104.155.42.1/geektalent'


# DATOIN endpoints:
# public domains: gt.datoin.com, gt1.datoin.com
# private IP: ???
# port: 8765
# endpoints: search, profiles

# search endpoint from DATOIN
DATOIN_SEARCH   = 'http://gt.datoin.com:8765/search'
# profiles endpoint from DATOIN
DATOIN_PROFILES = 'http://gt.datoin.com:8765/profiles'

# URL for Google Maps API
MAPS_API = 'https://maps.googleapis.com/maps/api/geocode/json'

# maximum number of profiles which get added to the DB
MAX_PROFILES    = 100

# minimum return value of phraseMatch for which constitutes match
SKILL_MATCHING_THRESHOLD = 0.75

# Number of digits to keep on Latitude/Longitude values
LATLON_DIGITS = 3

# Error margin for Latitude/Longitude lookups
LATLON_DELTA = 0.5*10**(-LATLON_DIGITS)

