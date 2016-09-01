from whichunidb import *
from dbtools import dict_from_row, row_from_dict
from logger import Logger
import sys
from windowquery import split_process, process_db
import argparse
from pprint import pprint
from sqlalchemy import func
import csv


def main():
    logger = Logger()
    wudb = WhichUniDB()
    subjects = wudb \
        .query(WUSubject) \
        .all()
    careers = wudb \
        .query(WUCareer) \
        .all()
    def fix_subject(subject):
        with open('whichuni/subject_mappings.csv') as smf:
            reader = csv.reader(smf)
            for row in reader:
                if(subject.title.lower() == row[0].lower()):
                    subject.title = row[1]
                    print("Changed subject {0} to {1}".format(row[0], row[1]))

    def fix_career(career):
        with open('whichuni/career_mappings.csv') as cmf:
            reader = csv.reader(cmf)
            for row in reader:
                if(career.title.lower() == row[0].lower()):
                    career.title = row[1]
                    print("Changed career {0} to {1}".format(row[0], row[1]))
    
    print("Fixing subjects")
    process_db(subjects, fix_subject, wudb, logger=logger) 
    print("Fixing careers")
    process_db(careers, fix_career, wudb, logger=logger)

if __name__ == "__main__":
    main()