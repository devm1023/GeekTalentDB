from datetime import datetime

class DateTimeParseError(ValueError):
    pass

def parse_datetime(s):
    """Convert a string into a datetime object.

    Valid formats are:

      YYYY-MM-DD
      YYYY-MM-DD HH
      YYYY-MM-DD HH:MM
      YYYY-MM-DD HH:MM:SS
      YYYY-MM-DD HH:MM:SS.mmmmmm

    Underscores are converted to spaces, leading and tailing spaces or
    underscores are stripped and the date and time parts can be separated by
    one or more spaces or underscores.

    The returned datetime objects are timezone-unaware.

    """
    tokens = s.replace('_', ' ').split()
    if not tokens:
        raise DateTimeParseError('blank time data')
    elif len(tokens) == 1:
        frm = '%Y-%m-%d'
    elif len(tokens) == 2:
        if '.' in tokens[1]:
            frm = '%Y-%m-%d %H:%M:%S.%f'
        else:
            timetokens = tokens[1].split(':')
            if len(timetokens) == 1:
                frm = '%Y-%m-%d %H'
            elif len(timetokens) == 2:
                frm = '%Y-%m-%d %H:%M'
            elif len(timetokens) == 3:
                frm = '%Y-%m-%d %H:%M:%S'
    else:
        raise DateTimeParseError('invalid time data {0:s}'.format(repr(s)))

    try:
        return datetime.strptime(' '.join(tokens), frm)
    except ValueError as e:
        raise DateTimeParseError(str(e))


if __name__ == '__main__':
    times = [
        ' _2016-09-17_  ',
        '2016-09-17_15',
        '2016-09-17_15:30',
        '2016-09-17 15:30:11',
        '2016-09-17 15:30:11.123456',
        '15:30:11',
        'dfsdfsdf',
        '  _ ',
        '',
    ]
    for time in times:
        try:
            print(parse_datetime(time))
        except DateTimeParseError as e:
            print(str(e))
