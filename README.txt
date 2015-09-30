GeekTalentDB -- Experimental SQL database for user profiles
===========================================================

The scripts operate on two PostgreSQL databases:

  datoin
    The raw (LinkedIn) data from DATOIN

  geektalent
    The processed data holding catalogues of skills, jobtitles, and locations
    (and eventually companies), and the skill ranks.

To clear all tables in one of the databases do

    pyton3 cleardb.py (datoin | geektalent)

To download LinkedIn data from DATOIN do

    python3 linkedin_download.py <njobs> <from-date> [<to-date>]

wher <njobs> is the number of parallel jobs to use, and <from-date> and
<to-date> specify the range of timestamps to download. The should be in the
format YYYY-MM-DD. If <to-date> is omitted the current date is used.

To populate the geektalent DB directly from DATOIN do

    python3 addlinkedin.py <start-date> <end-date>

See postgres-setup.txt for instructions to set up the PostgreSQL DB.
See python-setup.txt for instructions on installing python dependencies.


Files:

geektalentdb.py
  Contains SQLAlchemy classes describing the tables in the geektalent DB. Also
  provides the class GeekTalentDB to interact with the database. Data should
  only be added to the DB via one of the add_* methods of GeekTalentDB.

datoindb.py
  Contains SQLAlchemy classes describing the tables in the datoin DB. Also
  provides the class DatoinDB to interact with the database. Data should
  only be added to the DB via the add_liprofile method of DatoinDB.

conf.py, conf_example.py
  Global configurations such as URLs for database servers. The file
  conf_example.py holds an example configuration. The actual configuration
  must be put in conf.py. The file conf.py is NOT INDEXED by git, since it
  differs between installations.

cleardb.py
  Drops all tables and then re-constructs them. Does not add any data. Useful
  for starting with a clean slate.

download_linkedin.py
  Downloads LinkedIn profiles from DATOIN and adds them to the datoin DB.

addlinkedin.py
  Downloads LinkedIn profiles from DATOIN and adds them directly to the
  geektalent DB.

datoin.py
  Module for querying the DATOIN server(s).

sqldb.py
  Provides the base class (SQLDatabase) for DatoinDB and GeekTalentDB.

logger.py
  Provides a simple class for writing log messages.

phrasematch.py
  Provides a function (phraseMatch) for picking out phrases from free text.
  Also provides function for tokenizing and stemming phrases. Inteded for
  'normalizing' skills keywords.

parallelize.py
  Parallelisation module.

postgres-setup.txt
  Instructions for setting up the PostgreSQL databases.

python-setup.txt
  Instructions for installing the Python dependencies.

