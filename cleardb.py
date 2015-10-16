import conf
import sys

def usageAbort():
    print('usage: python3 cleardb.py (geektalent | datoin | normalform | geekmaps) [--no-create]')
    exit(1)    

nocreate = False
if len(sys.argv) > 2:
    if sys.argv[2] == '--no-create':
        nocreate = True
    else:
        usageAbort()
    
if sys.argv[1] == 'geektalent':
    from geektalentdb import GeekTalentDB
    gtdb = GeekTalentDB(url=conf.GEEKTALENT_DB)
    gtdb.drop_all()
    if not nocreate:
        gtdb.create_all()
elif sys.argv[1] == 'datoin':
    from datoindb import DatoinDB
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    dtdb.drop_all()
    if not nocreate:
        dtdb.create_all()
elif sys.argv[1] == 'canonical':
    from canonicaldb import CanonicalDB
    cndb = CanonicalDB(url=conf.CANONICAL_DB)
    cndb.drop_all()
    if not nocreate:
        cndb.create_all()
elif sys.argv[1] == 'geekmaps':
    from geekmapsdb import GeekMapsDB
    gmdb = GeekMapsDB(url=conf.GEEKMAPS_DB)
    gmdb.drop_all()
    if not nocreate:
        gmdb.create_all()
else:
    usageAbort()
