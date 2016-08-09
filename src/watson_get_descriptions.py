from canonicaldb import *
from watsondb import WatsonDB
from logger import Logger

import argparse


def main(args):
    cndb = CanonicalDB()
    wtdb = WatsonDB()
    logger = Logger()

    q = cndb.query(Entity.name) \
            .filter(Entity.source == 'linkedin',
                    Entity.type == 'skill',
                    Entity.language == 'en')
    if args.threshold is not None:
        q = q.filter(Entity.profile_count >= args.threshold)
    q = q.order_by(Entity.profile_count.desc())
    if args.limit is not None:
        q = q.limit(args.limit)

    for skill, in q:
        wtdb.get_descriptions(skill, lookup=True, logger=logger)


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None,
                        help='Maximum number of skills to look up.')
    parser.add_argument('--threshold', type=int, default=None,
                        help='Minimum number of mentions for a skill to be '
                        'looked up.')
    args = parser.parse_args()
    main(args)
        
