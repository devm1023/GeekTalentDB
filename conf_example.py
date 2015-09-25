USE_SPATIALDATA = True
USE_MYSQL       = False

# SQL Database used by scripts which write data
WRITE_DB_URL    = 'postgresql://geektalent:geektalent@localhost/geektalent'

# SQL Database used by scripts which only read data
READ_DB_URL     = 'postgresql://geektalent:Ta2tqaltuaatri42@104.155.42.1/geektalent'

# SQL DB URLs
# local db:        'postgresql://geektalent:geektalent@localhost/geektalent'
# internal server: 'postgresql://geektalent:geektalent@10.140.18.98/geektalent'
# cloud SQL:       'mysql+mysqldb://geektalent:geektalent@173.194.248.106/geektalent'
# cloud PostgreSQL 'postgresql://geektalent:Ta2tqaltuaatri42@104.155.42.1/geektalent'

# DATOIN endpoints:

# search endpoint from DATOIN
DATOIN_SEARCH   = 'http://172.31.42.48:8765/search'
# profiles endpoint from DATOIN
DATOIN_PROFILES = 'http://172.31.42.48:8765/profiles'

# public domains: gt.datoin.com, gt1.datoin.com
# private IP: 172.31.42.48
# port: 8765
# endpoints: search, profiles

# maximum number of profiles which get added to the DB
MAX_PROFILES    = 100

# minimum return value of phraseMatch for which constitutes match
SKILL_MATCHING_THRESHOLD = 0.75
