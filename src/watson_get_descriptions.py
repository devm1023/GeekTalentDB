import conf
from canonicaldb import *
from watsondb import WatsonDB
from logger import Logger

import argparse


def main(args):
    cndb = CanonicalDB(conf.CANONICAL_DB)
    wtdb = WatsonDB(conf.WATSON_DB)
    logger = Logger()

    q = cndb.query(Entity.name) \
            .filter(Entity.source == 'linkedin',
                    Entity.type == 'skill',
                    Entity.language == 'en') \
            .order_by(Entity.profile_count.desc())
    if args.limit is not None:
        q = q.limit(args.limit)

    for skill, in q:
        wtdb.get_descriptions(skill, lookup=True, logger=logger)


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None,
                        help='Maximum number of skills to look up.')
    args = parser.parse_args()
    main(args)
        
