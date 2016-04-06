import sys
import argparse
from logger import Logger
import analytics_build_catalogs
import analytics_build_profiles
import analytics_build_wordlists

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', default=1, type=int,
                        help='Number of parallel jobs for processing.')
    parser.add_argument('--batch-size', default=1000, type=int,
                        help='Batch size for parallel processing.')
    parser.add_argument('--source',
                        choices=['linkedin', 'indeed'],
                        help= 'Source type to process. If not specified all '
                        'sources are processed.')
    args = parser.parse_args()
    logger = Logger(sys.stdout)

    logger.log('\n\n>>>>> BUILDING CATALOGS <<<<<\n\n')
    args.catalog = None
    analytics_build_catalogs.main(args)

    logger.log('\n\n>>>>> BUILDING PROFILES <<<<<\n\n')
    args.from_id = None
    analytics_build_profiles.main(args)

    logger.log('\n\n>>>>> BUILDING WORD LISTS <<<<<\n\n')
    args.from_entity = None
    analytics_build_wordlists.main(args)
