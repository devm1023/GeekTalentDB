import sys
import argparse
from logger import Logger
import datoin_download_profiles
import canonical_parse_profiles
import canonical_geolookup

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-download', action='store_true',
                        help='Skip download from Datoin API.')    
    parser.add_argument('--no-geolookup', action='store_true',
                        help='Skip location lookup from Google Places.')
    parser.add_argument('--download-jobs', default=1, type=int,
                        help='Number of parallel jobs for downloading.')
    parser.add_argument('--process-jobs', default=1, type=int,
                        help='Number of parallel jobs for processing.')
    parser.add_argument('--geolookup-jobs', default=1, type=int,
                        help='Number of parallel jobs for location lookup.')
    parser.add_argument('--time-increment', default=1, type=int,
                        help='Time increment for parallel download in days.')
    parser.add_argument('--batch-size', default=1000, type=int,
                        help='Batch size for parallel processing.')
    parser.add_argument('--from-date', help=
                        'Only process profiles crawled or indexed on or after '
                        'this date. Format: YYYY-MM-DD',
                        default='1970-01-01')
    parser.add_argument('--to-date', help=
                        'Only process profiles crawled or indexed before\n'
                        'this date. Format: YYYY-MM-DD')
    parser.add_argument('--by-index-date', help=
                        'Indicates that the dates specified with --fromdate '
                        'and --todate are index dates. Otherwise they are '
                        'interpreted as crawl dates.',
                        action='store_true')
    parser.add_argument('--max-profiles', type=int,
                        help='Maximum number of profiles to download')
    parser.add_argument('--source',
                        choices=['linkedin', 'indeed', 'upwork', 'meetup',
                                 'github'],
                        help= 'Source type to process. If not specified all '
                        'sources are processed.')
    parser.add_argument('--skills', help=
                        'Name of a CSV file holding skill tags. Only needed '
                        'when processing Indeed CVs.')
    args = parser.parse_args()
    logger = Logger(sys.stdout)

    if not args.no_download:
        logger.log('\n\n>>>>> DOWNLOADING DATA <<<<<\n\n')
        args.jobs = args.download_jobs
        datoin_download_profiles.main(args)

    logger.log('\n\n>>>>> PARSING PROFILES <<<<<\n\n')
    args.jobs = args.process_jobs
    args.from_id = None
    canonical_parse_profiles.main(args)

    if not args.no_geolookup:
        logger.log('\n\n>>>>> LOOKING UP LOCATIONS <<<<<\n\n')
        args.jobs = args.geolookup_jobs
        args.from_location = None
        args.retry = None
        canonical_geolookup.main(args)
    
    
