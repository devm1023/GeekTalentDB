import conf
import sys

if len(sys.argv) < 2 or sys.argv[1] not in \
   ['geektalent', 'datoin', 'normalform']:
    print('usage: python3 cleardb.py (geektalent | datoin | normalform) [--no-create]')
    exit(1)

nocreate = False
if len(sys.argv) >= 3:
    nocreate = sys.argv[2] == '--no-create'
    
if sys.argv[1] == 'geektalent':
    from geektalentdb import GeekTalentDB
    gtdb = GeekTalentDB(url=conf.GT_WRITE_DB)
    gtdb.drop_all()
    if not nocreate:
        gtdb.create_all()
elif sys.argv[1] == 'datoin':
    from datoindb import DatoinDB
    dtdb = DatoinDB(url=conf.DT_WRITE_DB)
    dtdb.drop_all()
    if not nocreate:
        dtdb.create_all()
elif sys.argv[1] == 'normalform':
    from normalformdb import NormalFormDB
    nfdb = NormalFormDB(url=conf.NF_WRITE_DB)
    nfdb.drop_all()
    if not nocreate:
        nfdb.create_all()
