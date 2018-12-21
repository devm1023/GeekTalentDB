import argparse
from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys
import time

def get_script_dir():
    return str(Path(__file__).resolve().parent)

def run_parallel_jobs(jobs, num_parallel_jobs):
    ran_jobs = 0
    running_subprocesses = []

    while ran_jobs < len(jobs) or len(running_subprocesses):
        # check for finished jobs
        for job in running_subprocesses:
            id, sp, out, err = job
            ret = sp.poll()

            if ret is not None:
                print('Subprocess {} finished with code {} at {}'.format(id, ret, datetime.now().strftime('%d/%m/%Y %H:%M:%S')))
                running_subprocesses.remove(job)
                sp.communicate()
                out.close()
                err.close()

        # start new job(s)
        while len(running_subprocesses) < num_parallel_jobs and ran_jobs < len(jobs):
            out = open('./job_out{}'.format(ran_jobs), "w")
            err = open('./job_err{}'.format(ran_jobs), "w")

            # get full path to script (assume same directory)
            path = os.path.join(get_script_dir(), jobs[ran_jobs][0])

            # prepend python executable
            args = [sys.executable, path] + jobs[ran_jobs][1:]
        
            print ('Starting subprocess', ran_jobs, args, 'at', datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
            sp = subprocess.Popen(args, stdout=out, stderr=err)
            running_subprocesses.append((ran_jobs, sp, out, err))
            ran_jobs += 1

        time.sleep(1)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1, help='Number of jobs to run in parallel')
    parser.add_argument('--max-age', type=int, default=2)
    
    args = parser.parse_args()

    jobs = [
        # description crawler
        ['crawl_indeed_jobs.py', '--jobs', '4', '--crawl-rate', '1']
    ]

    countries = ['gb', 'fr', 'de', 'at', 'it', 'nl', 'pl', 'us']
    categories = ['it-jobs', 'engineering-jobs', 'healthcare-nursing-jobs', 'manufacturing-jobs', 'customer-services-jobs', 'admin-jobs', 'sales-jobs', 'hr-jobs', 'accounting-finance-jobs', 'part-time-jobs', 'hospitality-catering-jobs', 'logistics-warehouse-jobs', 'legal-jobs', 'teaching-jobs', 'retail-jobs', 'social-work-jobs', 'trade-construction-jobs', 'pr-advertising-marketing-jobs', 'creative-design-jobs']

    title_lists = {
        'it-jobs|gb': 'indeed_titles_skill_it.txt',
        'engineering-jobs|gb': 'indeed_titles_skill_eng.txt',
        'healthcare-nursing-jobs|gb': 'indeed_titles_skill_healthcare.txt',

        'it-jobs|fr': 'indeed_titles_skill_it_fr.txt',

        'it-jobs|de': 'indeed_titles_skill_it_de.txt'
    }

    for country in countries:
        for category in categories:
            # adzuna
            jobs.append(['get_adzuna_jobs.py', '--category', category, '--country', country, '--quiet', '--max-age', str(args.max_age)])

            # indeed
            title_list_key = '{}|{}'.format(category, country)

            if title_list_key not in title_lists:
                continue

            title_list_path = os.path.join(get_script_dir(), '../res/indeed/', title_lists[title_list_key])
            location_list_path = os.path.join(get_script_dir(), '../res/indeed/indeed_locations.csv')

            jobs.append(['get_indeed_jobs.py', '--category', category, '--country', country, '--titles-from', title_list_path,
                         '--locations-from', location_list_path, '--quiet', '--max-age', str(args.max_age)])

    # TODO: description URL import

    run_parallel_jobs(jobs, args.jobs)
