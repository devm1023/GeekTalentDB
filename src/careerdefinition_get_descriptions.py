import conf
from careerdefinitiondb import CareerDefinitionDB, Career, Sector
from descriptiondb import DescriptionDB
from logger import Logger
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-skills', type=int, default=25,
                        help='Maximum number of displayed skills. Default: 25')
    args = parser.parse_args()
    
    cddb = CareerDefinitionDB(conf.CAREERDEFINITION_DB)
    dscdb = DescriptionDB(conf.DESCRIPTION_DB)
    logger = Logger()
    
    q = cddb.query(Career, Sector.name) \
            .join(Sector) \
            .order_by(Sector.name, Career.title)
    for career, sector_name in q:
        logger.log('Getting skills for: {0:s} | {1:s}\n' \
                   .format(sector_name, career.title))
        for skill in career.skill_cloud[:args.max_skills]:
            description = dscdb.get_description(
                'skill', sector_name, skill.skill_name, watson_lookup=True,
                logger=logger)
        dscdb.commit()

