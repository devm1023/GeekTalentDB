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
    with parse.ParseDB() as psdb, WhichUniDB() as wudb:
        logger = Logger(sys.stdout)
        q = psdb.query(parse.WUUniversity) \
                .all()
        def add_university(row):
            university = dict_from_row(row)
            del university['id']
            university['tags'] = []
            university['characteristics'] = []
            existing_city = psdb.query(parse.WUCity) \
                                 .filter(parse.WUCity.id == university['city_id']) \
                                 .first()
            city = wudb.query(WUCity) \
                       .filter(WUCity.name == existing_city.name) \
                       .first()
            if city is None:
                city = wudb.add_from_dict({ 'name': existing_city.name}, WUCity)
                wudb.flush()
            university['city_id'] = city.id
            new_university = wudb.add_from_dict(university, WUUniversity)
            wudb.flush()
            for tag in row.tags:
                new_tag = wudb.query(WUTag) \
                              .filter(WUTag.name == tag.name) \
                              .first()
                if new_tag is None:
                    new_tag = WUTag(name=tag.name)
                    wudb.add(new_tag)
                    wudb.flush()
                university_tag = wudb.add_from_dict({
                    'university_id': new_university.id,
                    'tag_id': new_tag.id
                }, WUUniversityTag)
                wudb.commit()
            for characteristic in row.characteristics:
                new_characteristic = wudb.query(WUCharacteristic) \
                                         .filter(WUCharacteristic.name == characteristic.name) \
                                         .first()
                if new_characteristic is None:
                    new_characteristic = WUCharacteristic(name=characteristic.name)
                    wudb.add(new_characteristic)
                    wudb.flush()
                university_characteristic = wudb.add_from_dict({
                    'university_id': new_university.id,
                    'characteristic_id': new_characteristic.id,
                    'score': characteristic.score,
                    'score_r': characteristic.rating
                }, WUUniversityCharacteristic)
                wudb.commit()

        process_db(q, add_university, wudb, logger=logger)
if __name__ == "__main__":
    main()