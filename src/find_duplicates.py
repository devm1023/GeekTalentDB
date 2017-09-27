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

    d2 = {}
    for row2 in q2:
        str2 = strip_tags(row2.full_description)
        str2 = ' '.join(str2.split())
        d2[row2.id] = str2

    with open(args.out_file, 'w') as outputfile:
        csvwriter = csv.writer(outputfile)
        for row in q1:
            str1 = strip_tags(row.full_description)
            str1 = ' '.join(str1.split())
            if row.id % 10 == 0:
                print('{0} rows checked.'.format(row.id))
            for key, val in d2.items():
                if key > row.id:
                    delta = compare(str1, str2)
                    if delta > threshold:
                        print('Potential duplicates: {0} and {1} score: {2}\ntext1: {3}\ntext2: {4}'
                              .format(row.id, key, delta, row.full_description, str2))
                        csvwriter.writerow([row.id, key, delta, row.full_description, val])
    print('Done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--out_file', type=str,
                        help='CSV files where output will be written to.', default=None, required=True)
    parser.add_argument('--limit', type=int,
                        help='Threshold for a match to be significant. (0 - 100)', default=75)

    args = parser.parse_args()
    main(args)
