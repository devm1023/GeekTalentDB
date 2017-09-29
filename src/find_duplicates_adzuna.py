import requests
import argparse
from fuzzywuzzy import fuzz
import csv
import conf
import urllib.parse as url
from datoindb import *
from dbtools import dict_from_row
import collections
from html.parser import HTMLParser


"""
    This script deals with Adzuna duplicates within itself.
    Script calculates difference ratios and outputs those to a CSV file. 
"""

class MLStripper(HTMLParser):
    """Strips HTML from strings """
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def main(args):

    def compare(txt1, txt2):
        return fuzz.token_sort_ratio(txt1, txt2)

    threshold = args.limit

    dtdb = DatoinDB()

    q1 = dtdb.query(ADZJob).order_by(ADZJob.id)
    q2 = dtdb.query(ADZJob).order_by(ADZJob.id)

    d2 = dict(tuple())
    for row2 in q2:
        str2 = strip_tags(row2.full_description)
        str2 = ' '.join(str2.split())
        d2[row2.id] = (row2.location1, str2)

    with open(args.out_file, 'w') as outputfile:
        csvwriter = csv.writer(outputfile)
        for row in q1:
            str1 = strip_tags(row.full_description)
            str1 = ' '.join(str1.split())
            if row.id % 10 == 0:
                print('{0} rows checked.'.format(row.id))
            for key, val in d2.items():
                if key % 1000 == 0:
                    print('Outer:{0}, Inner: {1} working rows.'.format(row.id, key))
                if key > row.id:
                    if val[0] == row.location1 or val[0] is None:
                        delta = compare(str1, val[1])
                        if delta > threshold:
                            print('Potential duplicates: {0} and {1} score: {2}\ntext1: {3}\ntext2: {4}'
                                  .format(row.id, key, delta, row.full_description, val[1]))
                            csvwriter.writerow([row.id, key, delta, row.full_description, val[1]])

    # with open(args.out_file, 'w') as outputfile:
    #     csvwriter = csv.writer(outputfile)
    #     for row1 in q1:
    #         str1 = strip_tags(row1.full_description)
    #         str1 = ' '.join(str1.split())
    #         if row1.id % 10 == 0:
    #             print('{0} checked.'.format(row1.id))
    #         for row2 in q2:
    #             if row2.id % 1000 == 0:
    #                 print('Outer:{0}, Inner: {1} working rows.'.format(row1.id, row2.id))
    #             if row2.id > row1.id:
    #                 if row1.location1 == row2.location1 or row2.location1 is None:
    #                     str2 = strip_tags(row2.full_description)
    #                     str2 = ' '.join(str2.split())
    #                     delta = compare(str1, str2)
    #                     if delta > threshold:
    #                         print('Potential duplicates: {0} and {1} score: {2}\ntext1: {3}\ntext2: {4}'
    #                               .format(row1.id, row2.id, delta, row1.full_description, row2.full_description))
    #                         csvwriter.writerow([row1.id, row2.id, delta, row1.full_description, row2.full_description])
    print('Done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--out_file', type=str,
                        help='CSV files where output will be written to.', default=None, required=True)
    parser.add_argument('--limit', type=int,
                        help='Threshold for a match to be significant. (0 - 100)', default=75)

    args = parser.parse_args()
    main(args)
