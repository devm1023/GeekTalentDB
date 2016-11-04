# Created by: Aidanas Tamasauskas
# Created on: 2016-11-03
# Module provides means of mapping career titles to 'adzuna' titles.

__all__ = ['AdzunaMapper']


import csv
from textnormalization import normalized_entity


class AdzunaMapper:
    def __init__(self, filename):
        """
        The class reads CSV file and populates a local nested dictionary with title mappings.
        The top level key is the sector name. Internally, it uses normalised sector names for the mappings
        but titles are mapped unchanged so make sure CSV is correct.
        :param filename: CSV file containing mappings for Adzuna titles. Row format: sector, title, adzuna_title
        """
        if not filename:
            return

        self._titles_map = {None: {}}

        # Read the cvs file and populate the mappings directory.
        with open(filename, 'r') as csvfile:
            csvreader = csv.reader(row for row in csvfile if not row.strip().startswith('#'))

            for row in csvreader:
                row = list(filter(lambda s: s.strip(), row))

                # CVS file format error checking.
                if len(row) != 3 or not row[0] or not row[1] or not row[2]:
                    raise IOError('Invalid row in CSV file:\n{0:s}'.format(repr(row)))

                # Add career-title mapping using normalised names for sectors.
                nrm_sector = normalized_entity('sector', 'linkedin', 'en', row[0])
                if nrm_sector not in self._titles_map:
                    self._titles_map[nrm_sector] = {}
                self._titles_map[nrm_sector].update({row[1]: row[2]})


    def get_mappings_for_secotr(self, sector):
        """
        Get Adzuna title mappings for the sector.
        :param: sector: Name of the sector the titles belong to.
        :return  dict, {'Title1': 'Adzuna Title1'...}
        """
        nrm_sector = normalized_entity('sector', 'linkedin', 'en', sector)
        if nrm_sector not in self._titles_map:
            raise ValueError('No such sector in adzuna mapping! ({})'.format(sector))

        return self._titles_map[nrm_sector]


# Do self test if as a script.
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--adzuna-titles', dest='adzuna_title',
                        help='Name of a csv file holding Adzuna titles.'
                             'Columns: sector, title, adzuna_title', required=True)
    args = parser.parse_args()
    print('Self Testing Sequence Initiated')
    print('Parameter received : {}'.format(args.adzuna_title is not None))
    print('Parsing CSV file... ({})'.format(args.adzuna_title))
    adzuna_mapper = AdzunaMapper(args.adzuna_title)
    print('Parsed {}.'.format('OK' if adzuna_mapper is not None else 'FAILED'))
    title_mappings = adzuna_mapper.get_mappings_for_secotr('Digital Tech')
    print('Retrieving "Digital Tech" mappings : {}'.format('OK' if title_mappings is not None else 'FAIL'))
    print('Verifying a sample of title mappings...')

    # Change the sample data if cvs mappings change.
    title1 = 'Data Analyst'
    adz_title1 = 'Data Analyst Technology'
    title2 = 'Game Developer'
    adz_title2 = 'Game Developer'
    print('{:14} ---> {} : {}'.format(title1, adz_title1, 'OK' if title_mappings[title1] == adz_title1 else'FAIL'))
    print('{:14} ---> {} : {}'.format(title2, adz_title2, 'OK' if title_mappings[title2] == adz_title2 else'FAIL'))
    print('{:14} -/-> {} : {}'.format(title1, adz_title2, 'OK' if title_mappings[title2] != adz_title1 else'FAIL'))
