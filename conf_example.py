# SQL DB URLs
#
# Format:
# postgresql://<username>:<password>@<host>/<database>
#
# <username> is always 'geektalent'
# <database> is either 'geektalent' or 'datoin'
#
# host description   host name/IP      geektalent passwd    datoin passwd
# ---------------------------------------------------------------------------
# Martin's Laptop    localhost         geektalent           geektalent
# internal server    10.140.18.98      geektalent           geektalent
# cloud SQL server   173.194.248.106   geektalent           geektalent
# cloud PostgreSQL   104.155.42.1      Ta2tqaltuaatri42     Ta2tqaltuaatri42

# GeekTalent Database used by scripts which write data
GT_WRITE_DB    = 'postgresql://geektalent:geektalent@localhost/geektalent'

# GeekTalent Database used by scripts which only read data
GT_READ_DB     = 'postgresql://geektalent:Ta2tqaltuaatri42@104.155.42.1/geektalent'

# Datoin Database used by scripts which write data
DT_WRITE_DB    = 'postgresql://geektalent:geektalent@localhost/datoin'

# Datoin Database used by scripts which only read data
DT_READ_DB     = 'postgresql://geektalent:geektalent@localhost/datoin'


# Datoin SOLR endpoints:
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

