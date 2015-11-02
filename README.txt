GeekTalentDB -- Experimental SQL database for user profiles
===========================================================

The scripts operate on four PostgreSQL databases:

  datoin
    The raw (LinkedIn) data from DATOIN

  canonical
    Contains additional columns where short text elements like skills,
    job titles, companies and locations have been put in canonical form. This
    typically involves removal of 'funny' characters, lowercasing and sometimes
    stemming. Skills, job titles etc. are considered identical if their
    canonical form is identical.

    When building the 'canonical' DB the skill reinforcement is done and the
    ranks are stored in the table 'skill'. Latitudes and Longitudes are
    retreived from the Google Places API and stored in the table 'location'.

  analytics
    Holds catalogues of skills, job titles, companies, and locations as well as
    'skill cloud' tables describing the relationships between companies, job
    titles and skills.

  geekmaps
    Holds the data in a form suitable for visualisation with GeekMaps. Like
    'analytics' this database holds catalogues with skills, job titles, and
    companies found in the (LinkedIn) data and pre-computed NUTS IDs for each
    profile.

See postgres-setup.txt for instructions to set up the PostgreSQL DB.
See python-setup.txt for instructions on installing python dependencies.


Database initialisation
-----------------------

* To clear all tables in one of the databases do

    pyton3 initdb.py <database> [--no-create | --no-delete]

where <database> is datoin, canonical, analytics, or geekmaps. If the
--no-create flag is given the database is left completely blank (i.e. no empty
tables are created). If the --no-delete flag is given existing tables are not
dropped.


datoin
------

* To download LinkedIn data from DATOIN do

    python3 datoin_download_linkedin.py <njobs> <batchsize> \
        <from-date> <to-date> [<from-offset> [<to-offset>]]

where <njobs> is the number of parallel jobs to use, <batchsize> is the number
of profiles per job and iteration to download, and <from-date> and
<to-date> specify the range of timestamps to process. They should be in the
format YYYY-MM-DD. Optionally the range of offsets to download can be specified
with <from-offset> and <to-offset>. Example:

    python3 datoin_download_linkedin.py 20 100 2015-09-22 2015-09-23 0 10000


canonical
---------

* To populate the 'canonical' DB from the 'datoin' DB do

    python3 canonical_parse_linkedin.py <njobs> <batchsize> \
        <from-date> <to-date> [<from-id>]

The first four arguments are identical to datoin_download_linkedin.py. If the
optional argument <from-id> is given only profiles with a datoin ID of strictly
larger than <from-id> will be processed. This can be used for crash recovery,
since parse_linkedin.py writes the datoin ID of recently processed profiles to
STDOUT. Example:

    python3 canonical_parse_linkedin.py 4 200 2015-09-22 2015-09-23

* canonical_parse_linkedin.py does not populate the 'location' table in the
'canonical' DB. This needs to be done separately with

    python3 canonical_geolookup_linkedin.py <njobs> <batchsize> \
        <from-date> <to-date> [<from-location>]

The first four arguments are identical to datoin_download_linkedin.py. (The
range of timestamps now restricts the set of profiles whose locations are
included in the update.) The optional argument <from-location> can be used for
crash recovery in the same way as <from-id>. Example:

    python3 canonical_geolookup_linkedin.py 4 100 2015-09-22 2015-09-23


analytics
---------

* Before adding profile data to the 'analytics' DB you have to build the
catalogues of skills, job titles, and companies. Do

    python3 analytics_build_catalogs.py <njobs> <batchsize> \
        [(skills | titles | companies | locations) [<start-value>]]

The third argument specifies the catalogue to build. If it is omitted all
catalogues are built. As usual, the first two arguments control parallelisation
and the last one is for crash recovery. Example:

    python3 analytics_build_catalogs.py 4 200

* To add profile data do

    python3 analytics_build_liprofiles.py <njobs> <batchsize> [<start-value>]

Example:

    python3 analytics_build_liprofiles.py 4 500

* To build the skill clouds for job titles, companies and skills do

    python3 analytics_build_skillclouds.py <njobs> <batchsize> \
        [(titles | companies | skills) [<start-value>]]

The third argument specifies the type of skill cloud to build. If it is omitted
all skill clouds are built. Example

    python3 analytics_build_skillclouds.py 4 200


geekmaps
--------

* To create catalogues of skills, job titles and companies in the geekmaps DB do

    python3 geekmaps_build_catalogs.py <njobs> <batchsize> \
        [(skills | titles | companies) [<start-value>]]

If the third argument is not specified all catalogues are built. The optional
argument <start-value> can be used for crash recovery. Example:

    python3 geekmaps_build_catalogs.py 4 100

* To populate the geekmaps DB with pre-computed NUTS codes do

    python3 geekmaps_compute_nuts.py <njobs> <batchsize> <from-date> <to-date> \
        [<from-id>]

where <from-date> and <to-date> specify the range of timestamps of the profiles
to process. The optional last argument is for crash recovery as usual. Example:

    python3 geekmaps_compute_nuts.py 4 200 2015-09-22 2015-09-23


Additional files
----------------

datoindb.py, canonicaldb.py, geekmapsdb.py
  Contain SQLAlchemy classes describing the tables in the respective database.
  Also provide the classes DatoinDB, CanonicalDB, and GeekMapsDB to interact
  with the databases. Data should only be added to the databases via the
  correspoding add... methods.

conf.py, conf_example.py
  Global configurations such as URLs for database servers. The file
  conf_example.py holds an example configuration. The actual configuration
  must be put in conf.py. The file conf.py is NOT INDEXED by git, since it
  differs between installations.

datoin.py
  Module for querying the DATOIN server(s).

sqldb.py
  Provides the base class (SQLDatabase) for DatoinDB, CanonicalDB and GeekMapsDB.

logger.py
  Provides a simple class for writing log messages.

phrasematch.py
  Provides a function (phraseMatch) for picking out phrases from free text.
  Also provides function for tokenizing and stemming phrases. Inteded for
  'normalizing' skills keywords.

parallelize.py
  Basic parallelisation module.

windowquery.py
  Module for performing windowed SQL queries. Also provides the function
  splitProcess which takes care of the parallelisation in parse_linkedin.py,
  geoupdate_linkedin.py etc.

postgres-setup.txt
  Instructions for setting up the PostgreSQL databases.

python-setup.txt
  Instructions for installing the Python dependencies.

