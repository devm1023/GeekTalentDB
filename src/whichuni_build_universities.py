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
            existing_city = psdb.query(parse.WUCity) \
                                 .filter(parse.WUCity.id == university['city_id']) \
                                 .first()
            city = wudb.query(WUCity) \
                       .filter(WUCity.name == existing_city.name) \
                       .first()
            if city is None:
                city = wudb.add_from_dict({ 'name': existing_city.name}, WUCity)              
                wudb.flush()
            new_university = WUUniversity(name=university['name'],
                                            city_id=city.id,
                                            ucas_code=university['ucas_code'],
                                            courses_url=university['courses_url'],
                                            description=university['description'],
                                            website=university['website'],
                                            further_study=university['further_study'],
                                            further_study_r=university['further_study_r'],
                                            average_salary=university['average_salary'],
                                            average_salary_r=university['average_salary_r'],
                                            student_score=university['student_score'],
                                            student_score_r=university['student_score_r'],
                                            satisfaction=university['satisfaction'],
                                            no_of_students=university['no_of_students'],
                                            undergraduate=university['undergraduate'],
                                            postgraduate=university['postgraduate'],
                                            full_time=university['full_time'],
                                            part_time=university['part_time'],
                                            male=university['male'],
                                            female=university['female'],
                                            young=university['young'],
                                            mature=university['mature'],
                                            uk=university['uk'],
                                            non_uk=university['non_uk'],
                                            url=university['url'])
            wudb.add(new_university)         
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
            for league_table in row.league_tables:
                new_league_table = wudb.query(WULeagueTable) \
                                       .filter(WULeagueTable.name == league_table.name) \
                                       .first()
                if new_league_table is None:
                    new_league_table = WULeagueTable(name=league_table.name,
                                                    total=league_table.total)
                    wudb.add(new_league_table)
                    wudb.flush()
                university_league_table = wudb.add_from_dict({
                    'university_id': new_university.id,
                    'league_table_id': new_league_table.id,
                    'rating': league_table.rating
                }, WUUniversityLeagueTable)
                wudb.commit()

        process_db(q, add_university, wudb, logger=logger)
if __name__ == "__main__":
    main()