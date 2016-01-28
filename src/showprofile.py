import sys
import textwrap

def showProfile(profile, output=sys.stdout):
    output.write('ID:       {0:d}\n'.format(profile.id))
    output.write('DATOINID: {0:s}\n'.format(profile.datoinId))
    output.write('URL:      {0:s}\n'.format(profile.url))
    if hasattr(profile, 'pictureUrl') and profile.pictureUrl:
        output.write('PIC-URL:  {0:s}\n'.format(profile.pictureUrl))
    output.write('NAME:     {0:s}\n'.format(profile.name))
    if profile.company:
        output.write('COMPANY:  {0:s}\n'.format(profile.company))
    if profile.title:
        output.write('TITLE:    {0:s}\n'.format(profile.title))
    if profile.description:
        output.write(textwrap.fill(profile.description, 80,
                                   initial_indent='DESCRIPTION: ',
                                   subsequent_indent='    '))
        output.write('\n')

    # write experiences
    if profile.experiences:
        output.write('EXPERIENCES:\n')
    for experience in profile.experiences:
        output.write('....ID:       {0:d}\n'.format(experience.id))
        output.write('    DATOINID: {0:s}\n'.format(experience.datoinId))
        if experience.title:
            output.write('    TITLE:    {0:s}\n'.format(experience.title))
        if experience.company:
            output.write('    COMPANY:  {0:s}\n'.format(experience.company))
        if experience.start:
            output.write('    FROM:     {0:s}\n'\
                         .format(experience.start.strftime('%Y-%m-%d')))
        if experience.end:
            output.write('    TO:       {0:s}\n'\
                         .format(experience.end.strftime('%Y-%m-%d')))
        if experience.description:
            output.write(textwrap.fill(experience.description, 80,
                                       initial_indent='    DESCRIPTION: ',
                                       subsequent_indent='        '))
            output.write('\n')
        if experience.skills:
            output.write('    SKILLS:\n')
        for skill in experience.skills:
            output.write('        {0:s}\n'.format(skill.skill.name))

    # write educations
    if profile.educations:
        output.write('EDUCATIONS:\n')
    for education in profile.educations:
        output.write('....ID:       {0:d}\n'.format(education.id))
        output.write('    DATOINID: {0:s}\n'.format(education.datoinId))
        if education.institute:
            output.write('    INST:     {0:s}\n'.format(education.institute))
        if education.degree:
            output.write('    DEGREE:   {0:s}\n'.format(education.degree))
        if education.subject:
            output.write('    SUBJECT:  {0:s}\n'.format(education.subject))
        if education.start:
            output.write('    FROM:     {0:s}\n'\
                         .format(education.start.strftime('%Y-%m-%d')))
        if education.end:
            output.write('    TO:       {0:s}\n'\
                         .format(education.end.strftime('%Y-%m-%d')))
        if education.description:
            output.write(textwrap.fill(education.description, 80,
                                       initial_indent='    DESCRIPTION: ',
                                       subsequent_indent='        '))
            output.write('\n')

    # write groups
    if hasattr(profile, 'groups'):
        if profile.groups:
            output.write('GROUPS:\n')
        for group in profile.groups:
            output.write('    {0:s}\n'.format(group.name))
            
    # write skills
    if profile.skills:
        output.write(\
'SKILLS                                                            '
'REENF  SCORE\n')
    sortedskills = sorted(profile.skills, key=lambda s: (-s.score, s.name))
    for skill in sortedskills:
        output.write('    {0:60s}  {1: >5s}  {2: >5.1f}\n' \
                     .format(skill.name,
                             'Yes' if skill.reenforced else 'No',
                             skill.score))
    output.flush()

if __name__ == '__main__':
    import conf
    from canonicaldb import *
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='Data source',
                        choices=['linkedin', 'indeed'])
    parser.add_argument('-f', help='File with Datoin IDs (one per line)')
    parser.add_argument('id', help='Datoin ID(s)', nargs='*')
    args = parser.parse_args()

    ids = args.id[:]
    if args.f:
        with open(args.f, 'r') as textfile:
            for line in textfile:
                ids.append(line.strip())
    if not ids:
        exit(0)

    if args.source == 'linkedin':
        table = LIProfile
    elif args.source == 'indeed':
        table = INProfile
    else:
        raise ValueError('Invalid source.')
    
    cndb = CanonicalDB(url=conf.CANONICAL_DB)
    q = cndb.query(table).filter(table.datoinId.in_(ids))
    for n, profile in enumerate(q):
        sys.stdout.write('\n\n---- {0:d} ----\n\n'.format(n+1))
        showProfile(profile)

    
