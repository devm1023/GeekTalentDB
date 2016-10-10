import argparse
import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='The log file to generate a graph on')
    args = parser.parse_args()
    graph = []
    lst = open(args.file, 'r').readlines()
    for i, line in enumerate(lst):
        if line.startswith('Failed getting URL'):
            if lst[i + 1].startswith('Received status code 999'):
                failed_ip = line.split('http://geektalent:NEe2Yue6Jj@')[-1].replace('\n', '')
                found = False
                for i, item in enumerate(graph):
                    if item['ip'] == failed_ip:
                        graph[i]['count'] += 1;
                        found = True
                if not found:
                    graph.append({
                        'ip': failed_ip,
                        'count': 1
                    });
    index = np.arange(len(graph))
    x = [a['ip'] for a in graph]
    y = np.array([a['count'] for a in graph])
    plt.bar(index, y, color="blue")
    plt.xticks(index, x)
    plt.show()