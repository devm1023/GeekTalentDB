'''
Updates career counts with SOC data 🧦
'''
import csv
import argparse

from careerdefinitiondb import CareerDefinitionDB, Career, Sector


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('titles_file',
                        help='CSV file mapping titles to SOC codes.')
    parser.add_argument('soc_data_file',
                        help='CSV file with SOC code data.')
    args = parser.parse_args()

    cddb = CareerDefinitionDB()

    title_socs = {}
    soc_counts = {}

    # title/soc mapping
    with open(args.titles_file, 'r') as titles_file:
        csv_reader = csv.reader(titles_file)
        for row in csv_reader:
            title_socs[row[0]] = row[1]

    # counts
    with open(args.soc_data_file, 'r') as socs_file:
        csv_reader = csv.reader(socs_file)
        for row in csv_reader:
            if row[1] != '':
                soc_counts[row[0].strip()] = int(row[1])


    q = cddb.query(Career)

    for row in q:

        sector = cddb.query(Sector).filter(Sector.id == row.sector_id).first()

        # don't change any of our data
        if sector.datatype != 'ONS':
            print('"{}" is not an ONS sector'.format(sector.name))
            continue

        if row.title not in title_socs:
            print('No soc for "{}"'.format(row.title))
            continue

        soc_code = title_socs[row.title]

        if soc_code not in soc_counts:
            print('No count for soc {} ("{}")'.format(soc_code, row.title))
            continue

        count = soc_counts[soc_code]

        # update
        #print('Updated count for "{}" from {} to {}'.format(row.title, row.count, count))
        row.count = count

    cddb.commit()