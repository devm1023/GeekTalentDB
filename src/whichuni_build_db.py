from whichunidb import *
import parsedb as parse
from dbtools import dict_from_row, row_from_dict
from logger import Logger
import sys
from windowquery import split_process, process_db
import argparse
from pprint import pprint
from sqlalchemy import func

def main():
    psdb = parse.ParseDB()
    wudb = WhichUniDB()
    logger = Logger(sys.stdout)
    q = psdb.query(parse.WUSubject) \
           .distinct(parse.WUSubject.title) \
           .all()
    def add_subject(subject):
        s = dict_from_row(subject)
        del s['id']
        s['alevels'] = []
        s['careers'] = []
        s['average_salary'] = s['average_salary'] \
            .replace('£', '') \
            .replace('k', '')
        s['average_salary'] = int(float(s['average_salary']) * 1000)
        s['employed_furtherstudy'] = float(s['employed_furtherstudy'] \
            .replace('%', ''))
        new_subject = wudb.add_from_dict(s, WUSubject)
        wudb.flush()
        for alevel in subject.alevels:
            new_alevel = wudb.query(WUALevel) \
                     .filter(func.lower(WUALevel.title)
                             == func.lower(alevel.title)) \
                     .first()
            if new_alevel is None:
                new_alevel = WUALevel(title=alevel.title)
                wudb.add(new_alevel)
                wudb.flush()
            subject_alevel = wudb.add_from_dict({
                "subject_id": new_subject.id,
                "alevel_id": new_alevel.id
            }, WUSubjectALevel)
            wudb.commit()
        for career in subject.careers:
            new_career = wudb.query(WUCareer) \
                             .filter(func.lower(WUCareer.title)
                             == func.lower(career.title)) \
                             .first()
            if new_career is None:
                new_career = WUCareer(title=career.title)
                wudb.add(new_career)
                wudb.flush()
            subject_career = wudb.add_from_dict({
                "subject_id": new_subject.id,
                "career_id": new_career.id
            }, WUSubjectCareer)
            wudb.commit()
        final_subject = wudb.query(WUSubject) \
                            .filter(WUSubject.id == new_subject.id) \
                            .first()
        print("Processed {0}".format(final_subject.title))
    process_db(q, add_subject, wudb, logger=logger)


if __name__ == "__main__":
    main()