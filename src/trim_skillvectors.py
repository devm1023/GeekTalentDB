import argparse
import csv

def main(args):

    with open(args.input_file) as in_file, open(args.output_file, 'w') as out_file:
        csv_reader = csv.reader(in_file)
        csv_writer = csv.writer(out_file)

        cur_title = None
        trimmed = 0
        copied = 0
        for row in csv_reader:
            if not row:
                continue

            if row[0] == 't':
                if cur_title is not None:
                    print('{}: trimmed {}, copied {}'.format(cur_title, trimmed, copied))

                cur_title = row[2]
                trimmed = 0
                copied = 0
            elif row[0] == 's':
                if float(row[2]) < args.threshold:
                    trimmed += 1
                    continue
                else:
                    copied += 1

            csv_writer.writerow(row)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--threshold', type=float, default=0.01)
    parser.add_argument('input_file')
    parser.add_argument('output_file')
    args = parser.parse_args()
    main(args)