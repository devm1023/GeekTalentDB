from logger import Logger
import argparse
from parse_datetime import parse_datetime
import datetime
import re
from pprint import pprint
import matplotlib.pyplot as plt
import datetime
import numpy as np

non_decimal = re.compile(r'[^\d.]+')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='The log file to generate a graph on')
    args = parser.parse_args()
    graph = []
    lst = open(args.file, 'r').readlines()
    for i, line in enumerate(lst):
        if line.startswith('Starting batch at'):
            start_time = line[18:-2]
            if len(lst) is not i + 2:
                if lst[i + 1].startswith('Retreived URLs at'):
                    end_time = lst[i + 1][18:-2]
                    if lst[i + 2].startswith('Crawled'):
                        crawled_no = non_decimal.sub('', lst[i + 2][:-50])
                        crawl_rate = float(non_decimal.sub('', lst[i + 2][40:-11]))
                        success_rate = non_decimal.sub('', lst[i + 2][30:-20])
                        crawl_length = parse_datetime(end_time) - parse_datetime(start_time)
                        graph.append({
                            'start_time': parse_datetime(start_time),
                            'start_time_string': start_time,
                            'end_time': parse_datetime(end_time),
                            'end_time_string': end_time,
                            'crawl_rate': crawl_rate,
                            'success_rate': int(success_rate),
                            'crawled_no': int(float(crawled_no)),
                            'crawl_length': int(crawl_length.total_seconds())
                        })
    x = np.array([a['start_time'] for a in graph])
    y = np.array([a['crawl_rate'] for a in graph])
    crawl_start_time = graph[0]['start_time_string']
    crawl_end_time = graph[-1]['end_time_string']
    title_string = 'Crawl starting {0} and ending {1}'.format(crawl_start_time, crawl_end_time)
    fig = plt.figure()
    plt.plot(x,y)
    fig.suptitle(title_string, fontsize=16)
    plt.ylim(ymax = 1, ymin = 0)
    plt.show()