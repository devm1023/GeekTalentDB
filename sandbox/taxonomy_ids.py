import csv

def makeIds(taxonomy):
    taxonomy = taxonomy[:]
    while taxonomy and not taxonomy[-1]:
        del taxonomy[-1]
    
    dirs = []
    for t in taxonomy:
        t = t.replace(' ', '_') \
             .replace(',', '') \
             .replace('-', '') \
             .replace('\'', '') \
             .replace('+', '')
        if t:
            dirs.append(t)

    ids = []
    while taxonomy:
        ids.append(('/'.join(taxonomy), '.'.join(dirs)))
        dirs.pop(-1)
        taxonomy.pop(-1)
    
    return ids


rows = set()
with open('watson_taxonomies_CH_v1.csv', 'r') as csvinput:
    csvreader = csv.reader(csvinput)
    next(csvreader)
    for row in csvreader:
        if len(row) < 5:
            continue
        row = row[:5]
        rows.update(makeIds(row))

rows = list(rows)
rows.sort()
with open('taxonomy_ids.csv', 'w') as csvoutput:
    csvwriter = csv.writer(csvoutput)
    for row in rows:
        csvwriter.writerow(row)
