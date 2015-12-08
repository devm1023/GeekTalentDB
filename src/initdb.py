import conf
import sys

def usageAbort():
    print('usage: python3 initdb.py (geektalent | datoin | canonical | analytics | geekmaps) [--no-create | --no-delete]')
    exit(1)    

nocreate = False
nodelete = False
if len(sys.argv) > 2:
    if sys.argv[2] == '--no-create':
        nocreate = True
    elif sys.argv[2] == '--no-delete':
        nodelete = True
    else:
        usageAbort()
    
if sys.argv[1] == 'geektalent':
    from geektalentdb import GeekTalentDB
    gtdb = GeekTalentDB(url=conf.GEEKTALENT_DB)
    if not nodelete:
        gtdb.drop_all()
    if not nocreate:
        gtdb.create_all()
elif sys.argv[1] == 'datoin':
    from datoindb import DatoinDB
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    if not nodelete:
        dtdb.drop_all()
    if not nocreate:
        dtdb.create_all()
elif sys.argv[1] == 'canonical':
    from canonicaldb import CanonicalDB
    cndb = CanonicalDB(url=conf.CANONICAL_DB)
    if not nodelete:
        cndb.drop_all()
    if not nocreate:
        cndb.create_all()
elif sys.argv[1] == 'analytics':
    from analyticsdb import AnalyticsDB
    andb = AnalyticsDB(url=conf.ANALYTICS_DB)
    if not nodelete:
        andb.drop_all()
    if not nocreate:
        andb.create_all()
elif sys.argv[1] == 'geekmaps':
    from geekmapsdb import GeekMapsDB
    gmdb = GeekMapsDB(url=conf.GEEKMAPS_DB)
    if not nodelete:
        gmdb.drop_all()
    if not nocreate:
        gmdb.create_all()
else:
    usageAbort()
