USE_SPATIALDATA = True
USE_MYSQL       = False

# SQL DB URLs
# local db:        'postgresql://geektalent:geektalent@localhost/geektalent'
# internal server: 'postgresql://geektalent:geektalent@10.140.18.98/geektalent'
# cloud SQL:       'mysql+mysqldb://geektalent:geektalent@173.194.248.106/geektalent'
# cloud PostgreSQL 'postgresql://geektalent:Ta2tqaltuaatri42@104.155.42.1/geektalent'
WRITE_DB_URL    = 'postgresql://geektalent:geektalent@localhost/geektalent'
READ_DB_URL     = 'postgresql://geektalent:Ta2tqaltuaatri42@104.155.42.1/geektalent'

# SOLR servers:
# snapshot:   'http://10.140.18.98:8983/solr/gt-search/select'
# Umar:       'http://54.148.225.122:8983/solr/gt-search/select'
SOLR_URL        = 'http://10.140.18.98:8983/solr/gt-search/select'

# DATOIN endpoints:
DATOIN_SEARCH   = 'http://gt1.datoin.com:8765/search'
DATOIN_PROFILES = 'http://gt1.datoin.com:8765/profiles'

MAX_PROFILES    = 100
SKILL_MATCHING_THRESHOLD = 0.75
