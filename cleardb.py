import conf
from geektalentdb import *

gtdb = GeekTalentDB(url=conf.WRITE_DB_URL)
gtdb.drop_all()
gtdb.create_all()

