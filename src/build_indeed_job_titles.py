import argparse
import csv

from canonicaldb import CanonicalDB, Entity

"""
   Script builds a file to be used by get_indeed_jobs.py as titles input file.
   The script uses a skill vector file as an input. 
"""

def skill_name(cndb, nrm_skill):
    name = cndb.query(Entity.name).filter(Entity.nrm_name == nrm_skill).first()
    if name is not None:
        return name[0]
    return nrm_skill.split(':')[-1]

def main(args):
    cndb = CanonicalDB()

    skill_limit = args.skill_limit
    titles = {}
    with open(args.sv, 'r') as infile:
        csvreader = csv.reader(infile)
        for row in csvreader:
            if len(row) < 3:
                continue
            if row[0] == 't':
                skills = []
                titles[row[2]] = skills
            else:
                if len(skills) >= skill_limit:
                    continue
                if args.destem_skills:
                    skill = skill_name(cndb, row[1])
                else:
                    skill = row[1].split(':')[-1]
                skills.append(skill)

    if args.format == 'title':
        with open('indeed_titles_title.txt', 'w') as outputfile:
            for title in titles:
                outputfile.write('title:({0})\n'.format(title))
    elif args.format == 'skill':
        with open('indeed_titles_skill.txt', 'w') as outputfile:
            for title in titles:
                skills_list = ' or '.join(titles[title])
                outputfile.write('"{0}" ({1})\n'.format(title, skills_list))
    else:
        raise Exception('Output format unspecified!')

    print('Titles read: {0}'.format(len(titles)))
    print('Done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sv', type=str,
                        help='Skill vectors file with titles.', default=None, required=True)
    parser.add_argument('--format', type=str,
                        help='Format of the titles to output. This is specific to indeed query parameter',
                        choices=['title', 'skill'], default='title')
    parser.add_argument('--skill_limit', type=int,
                        help='Number of most relevant skills to add. Only applicable with --format skill option.',
                        default=5)
    parser.add_argument('--destem-skills', type=bool,
                        help='Remove stemming from skills by looking up names in the Entity table. Only applicable with --format skill option.',
                        default=False)
    args = parser.parse_args()
    main(args)
