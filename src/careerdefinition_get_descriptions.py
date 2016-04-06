import conf
from careerdefinitiondb import *
from logger import Logger

if __name__ == '__main__':
    cddb = CareerDefinitionDB(conf.CAREERDEFINITION_DB)
    logger = Logger()
    
    q = cddb.query(Career) \
            .order_by(Career.linkedin_sector, Career.title)
    for career in q:
        logger.log('{0:s} | {1:s}\n'.format(career.linkedin_sector,
                                            career.title))
        cddb.get_descriptions(career)
        cddb.commit()

