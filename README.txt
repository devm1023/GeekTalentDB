GeekTalentDB -- Experimental SQL database for user profiles
===========================================================

Files:

geektalentdb.py
  Contains SQLAlchemy classes describing the tables in the DB. Also provides
  the class GeekTalentDB to interact with the database. Data should only be
  added to the DB via one of the add_* methods of GeekTalentDB.

conf.py, conf_example.py
  Global configurations such as URLs for database servers. The file
  conf_example.py holds an example configuration. The actual configuration
  must be put in conf.py. The file conf.py is NOT INDEXED by git, since it
  differs between installations.

cleardb.py
  Drops all tables and then re-constructs them. Does not add any data. Useful
  for starting with a clean slate.
  
addlinkedin.py
  Downloads LinkedIn profiles form DATOIN and adds them to the DB. Example:

      python3 addlinkedin.py 2015-01-01 2015-08-31

  The dates specify the range of timestamps to download. The last date can be
  omitted.

datoin.py
  Module for querying the DATOIN server(s).

logger.py
  Provides a simple class for writing log messages.

phrasematch.py
  Provides a function (phraseMatch) for picking out phrases from free text.
  Also provides function for tokenizing and stemming phrases. Inteded for
  'normalizing' skills keywords.



  