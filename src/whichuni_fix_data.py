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
                    logger.log("Changed subject {0} to {1}\n".format(row[0], row[1]))

    def fix_career(career):
        with open('whichuni/career_mappings.csv') as cmf:
            reader = csv.reader(cmf)
            for row in reader:
                if(career.title.lower() == row[0].lower()):
                    career.title = row[1]
                    logger.log("Changed career {0} to {1}\n".format(row[0], row[1]))
    
    def add_careers_to_subject(subject):
        with open('whichuni/added_careers.csv') as acf:
            reader = csv.reader(acf)
            rows = [row for row in reader if row[0].lower() == subject.title.lower()]
            for row in rows:
                career = wudb \
                    .query(WUCareer) \
                    .filter(func.lower(WUCareer.title) == func.lower(row[1])) \
                    .first()
                subject = wudb \
                    .query(WUSubject) \
                    .filter(func.lower(WUSubject.title) == func.lower(row[0])) \
                    .first()
                if career is None:
                    career = WUCareer(title=row[1])
                    wudb.add(career)
                    wudb.flush()
                wu_subject_career = wudb.query(WUSubjectCareer) \
                    .filter(WUSubjectCareer.career_id == career.id) \
                    .filter(WUSubjectCareer.subject_id == subject.id) \
                    .first()
                if wu_subject_career is None:
                    wu_subject_career = WUSubjectCareer(career_id=career.id, subject_id=subject.id)
                    wudb.add(wu_subject_career)
                    wudb.flush()
                    logger.log("Added career {0} to {1}\n".format(career.title, subject.title))
    
    print("Fixing subjects")
    process_db(subjects, fix_subject, wudb, logger=logger) 
    process_db(subjects, add_careers_to_subject, wudb, logger=logger)
    print("Fixing careers")
    process_db(careers, fix_career, wudb, logger=logger)

if __name__ == "__main__":
    main()