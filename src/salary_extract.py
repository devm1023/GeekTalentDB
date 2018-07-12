import re

from textnormalization import clean

salary_regex = re.compile(r'[£\-–] ?([0-9,.\'’]+)(k?)', re.IGNORECASE) # match both values in something like "£1-2k"
salary_range_regex_pre = re.compile(r'[£0-9]+(p.|k|p/.)?-?$', re.IGNORECASE) # comma and . are handled by clean
salary_range_regex_post = re.compile(r'^[£0-9]+')

number_regex = re.compile(r'^\d+([,\'’]\d{3})*(\.\d+)?$')

def check_salary_range(words, is_pre, offset = 0):
    delim = words[offset]

    if len(words) > offset + 1:
        next = words[offset + 1]
    else:
        next = None

    regex = salary_range_regex_pre if is_pre else salary_range_regex_post

    # "£1234 to £2345", "between £1234 and £2345"
    if delim == 'to' or delim == 'and':
        # avoid marking "salary: £1234 to apply..." as a min salary
        if next is not None and (is_pre or next[0] == '£') and regex.match(next):
            return True

        return False

    # "£1234-£2345"
    has_hyphen = (is_pre and delim.endswith('-')) or (not is_pre and delim.startswith('-'))
    check_for_number = next
    if len(delim) > 1: # no space around seperator
        check_for_number = delim.strip('-')

    # avoid marking "salary: £1234 - some other details" as a min salary
    if has_hyphen and check_for_number is not None and regex.search(check_for_number) is not None:
        return True
    
    return False

def extract_salary(text):
    last_value = None
    last_was_min = False

    any_has_keyword = False

    salary_matches = set()
    found_periods = set()

    for match in salary_regex.finditer(text):
        # badly formatted number
        if not number_regex.match(match.group(1).strip('.,')):
            continue

        value = float(match.group(1).strip('.').replace(',', '').replace('\'', '').replace('’', ''))

        # multiply if ending in k
        if match.group(2).lower() == 'k' and value < 10000: # avoid unreasonably high salaries from typos like '35,000k'
            value *= 1000

        span = match.span()

        # get some context
        min_ctx = max(0, span[0] - 50)
        max_ctx = span[1] + 50

        context_pre = clean(text[min_ctx:span[0]].replace('–', '-'), lowercase=True, tokenize=True, keep='-/£')
        context_post = clean(text[span[1]:max_ctx].replace('–', '-'), lowercase=True, tokenize=True, keep='-/£')

        # remove first word as it's likely truncated
        if min_ctx > 0:
            context_pre = context_pre[1:]

        # same for last word
        if max_ctx < len(text):
            context_post = context_post[:-1]

        context_pre_rev = context_pre[::-1]

        # try to avoid overtime rates and bonuses
        if 'overtime' in context_pre_rev[:3] or 'bonus' in context_pre_rev[:3] or (context_post and context_post[0] == 'bonus' and not last_was_min):
            continue

        has_keyword = 'salary' in context_pre or 'pay' in context_pre or 'paying' in context_pre or 'remuneration' in context_pre

        # make sure trailing value is actually part of a salary range
        if match.group(0)[0] != '£':
            # nothing before
            if not context_pre_rev:
                continue

            # concat until we hit a non-number
            prev = context_pre_rev[0]
            i = 0
            while prev.isdigit() and i < len(context_pre_rev):
                prev = context_pre_rev[i] + prev
                i += 1

            # check for currency prefix
            if prev.strip('-')[0] != '£':
                continue

            # add the seperator back into the context
            context_pre.append('-')
            context_pre_rev.insert(0, '-')

        # check for period
        possible_period = None
        period = None

        post_offset = 0

        # "per hour", "/ hour" (with space)
        if len(context_post) > 1 and (context_post[0] == 'per' or context_post[0] == '/' or context_post[0] == 'p'):
            possible_period = context_post[1]
            post_offset = 2
        # "/hour" (no space)
        elif context_post and context_post[0][0] == '/' and len(context_post[0]) > 1:
            possible_period = context_post[0][1:]
            post_offset = 1
        elif context_post and (context_post[0] == 'pa' or context_post[0] == 'p/a'):
            possible_period = 'year'
            post_offset = 1
        elif context_post and (context_post[0] == 'pd'):
            possible_period = 'day'
            post_offset = 1
        elif context_post and (context_post[0] == 'ph' or context_post[0] == 'p/h' or context_post[0] == 'per/hour'):
            possible_period = 'hour'
            post_offset = 1

        # normalise a bit
        if possible_period is not None:
            possible_period = possible_period.replace('/', '').replace('-', '')
        if possible_period in ['year', 'month', 'week', 'day', 'hour']:
            period = possible_period
        elif possible_period == 'hr' or possible_period == 'h':
            period = 'hour'
        elif possible_period == 'annum' or possible_period == 'a':
            period = 'year'

        # check for ranges ("£1-£2", "£3 to £4")
        is_min = False
        is_max = False

        if not last_was_min and len(context_post) > post_offset and check_salary_range(context_post, False, post_offset):
            is_min = True
        elif last_was_min and context_pre_rev and check_salary_range(context_pre_rev, True):
            is_max = True

        # remove unreasonably large ranges
        if value > 1000000 or (context_post and (context_post[0] == 'million' or context_post[0] == 'm' or context_post[0] == 'billion')):
            # remove the min if this is a max
            if is_max and last_was_min:
                for existing in salary_matches:
                    ex_val, ex_period, ex_min, ex_max, ex_kw = existing
                    if ex_val == last_value and ex_min:
                        salary_matches.remove(existing)
                        break

            continue

        # assume anything > 10k is yearly (if in a range or with a keyword)
        if period is None and value > 10000 and (is_min or is_max or has_keyword):
            period = 'year'

        # propogate period from max to min
        if is_max and last_was_min and period is not None:
            for existing in salary_matches:
                ex_val, ex_period, ex_min, ex_max, ex_kw = existing
                if ex_val == last_value and ex_min:
                    salary_matches.remove(existing)
                    salary_matches.add((ex_val, period, ex_min, ex_max, ex_kw))
                    break

        # add to set

        # merge with an existing entry if more defined (has keyword, period)
        to_remove = []
        for existing in salary_matches:
            ex_v, ex_p, ex_min, ex_max, ex_has_keyword = existing
            if (ex_v, ex_min, ex_max) == (value, is_min, is_max) and (ex_p == period or ex_p is None):
                to_remove.append(existing)

        for r in to_remove:
            salary_matches.remove(r)
                
        salary_matches.add((value, period, is_min, is_max, has_keyword))

        #if period is not None:
        found_periods.add(period)

        any_has_keyword = has_keyword or any_has_keyword

        # track last
        last_value = value
        last_was_min = is_min

    # validate
    validation_error = False
    min_salary = None
    max_salary = None
    salary_period = None
    keyword_values = []

    for value, period, is_min, is_max, has_keyword in salary_matches:
        if salary_period is None:
            salary_period = period
        elif period is not None and salary_period != period:
            validation_error = True

        # allow 1000x too small as we might be missing a "k"
        if is_min and min_salary is None or min_salary == value * 1000:
            min_salary = value
        elif is_min and min_salary != value and min_salary * 1000 != value:
            validation_error = True

        if is_max and max_salary is None:
            max_salary = value
        elif is_max and max_salary != value:
            validation_error = True

        if has_keyword:
            keyword_values.append(value)

    min_max_has_kw = min_salary in keyword_values or max_salary in keyword_values

    # try only with a keyword it there are errors
    need_revalidate = False
    min_or_max_missing = min_salary is None or max_salary is None

    if any_has_keyword and (validation_error or min_or_max_missing or not min_max_has_kw) and len(salary_matches) > 1:
        salary_matches = set([x for x in salary_matches if x[4]])
        need_revalidate = True
        found_periods = {x[1] for x in salary_matches}

    # take only one period if there are multiple
    if (validation_error or min_or_max_missing) and len(found_periods) > 1 and len(salary_matches) > 1:
        preferred_period = None
        if 'year' in found_periods:
            preferred_period = 'year'
        elif 'hour' in found_periods:
            preferred_period = 'hour'

        if preferred_period is not None:
            salary_matches = set([x for x in salary_matches if x[1] == preferred_period])
            need_revalidate = True

    if need_revalidate:
        validation_error = False
        min_salary = max_salary = salary_period = None

        for value, period, is_min, is_max, has_keyword in salary_matches:
            if salary_period is None:
                salary_period = period
            elif period is not None and salary_period != period:
                validation_error = True

            if is_min and min_salary is None:
                min_salary = value
            elif is_min and min_salary != value:
                validation_error = True

            if is_max and max_salary is None:
                max_salary = value
            elif is_max and max_salary != value:
                validation_error = True

    # use single value for both min and max
    if len(salary_matches) == 1 and min_salary is None and max_salary is None:
        min_salary, salary_period, _, _, _ = next(iter(salary_matches)) # get only element
        max_salary = min_salary

        if salary_period is None and min_salary > 10000:
            salary_period = 'year'

    # final salary
    # accept both min and max or one of the two, but only if there are no other values
    have_min_and_max = min_salary is not None and max_salary is not None
    have_min_or_max = min_salary is not None or max_salary is not None

    # handle cases where the salary range was written as "£1-2k"
    if have_min_and_max and min_salary * 1000 < max_salary:
        min_salary *= 1000

    if not validation_error and (have_min_and_max or (have_min_or_max and len(salary_matches) == 1)):
        return (min_salary, max_salary, salary_period)

    return None

# code coverage:
# python3 -m coverage run --branch ../src/salary_extract.py
# python3 -m coverage html --omit "/usr/lib/*,../src/textnormalization.py"
if __name__ == '__main__':
    test_cases = [
        ('', None),
        # "-" without spaces 
        ('Location: Warwick, West Midlands, UK Job Salary: £20-£22 per hour (dependent upon experience) Position Des', (20.0, 22.0, 'hour')),
        ('Location: Bristol £30,000-40,000 Key Skills: Software, embedded, C, GUI, LabWindow', (30000.0, 40000.0, 'year')),
        # "-" with spaces
        ('Salary: £28,000 - £31,500 plus local government pension, training, generous', (28000.0, 31500.0, 'year')),
        # "x to y"
        (
            '''011633
Job Title: Responsible Engineer
Rate: From £38.00 to £44.00 per Hour Ltd Co.

The Company:''', 
            (38.0, 44.0, 'hour')
        ),
        # additional "up to" value 
        (
            '''– Newcastle - £35,000 – £50,000 Embedded Software Engineer is required to join a 
any. This is a permanent opportunity paying up to £50,000. As a Embedded Software Engineer you must have exp''',
            (35000.0, 50000.0, 'year')
        ),
        # and with non-salary
        ('Would attract salary of £31,200 and 20 days annual leave', (31200.0, 31200.0, 'year')),
        # no range
        ('Salary £38000 Monday to Friday – 6am -2:30pm Famous Building Th', (38000.0, 38000.0, 'year')),
        # number - salary
        ('Multi Skilled Maintenance Engineers x 2 - £38,500', (38500.0, 38500.0, 'year')),
        
        # "k" on min and max
        ('Welwyn Garden City. The role offers an attractive £45k-£55k salary with excellent benefits including 25 days ', (45000.0, 55000.0, 'year')),
        # "k" only on max
        (' a permanent staff basis with a salary banding of £35-£45k.', (35000.0, 45000.0, 'year')),
        # extra "k"
        ('£30,000 - £35,000k + excellent benefits & great company culture', (30000.0, 35000.0, 'year')),
        # "£" only on min
        ('iday 8.30am – 5pm. Salary depending on experience £15-23k. To apply for this position, please send your cv', (15000.0, 23000.0, 'year')),
        ('Salary -£30-35k+ Outstanding Benefits', (30000.0, 35000.0, 'year')),
        # no "k"s then only on max
        (
            '''For this role we can offer £45,000 - 55,000 + company car or allowance
...
Technical Project Manager - £45-55k - Central Nottingham''',
            (45000.0, 55000.0, 'year')
        ),
        # ^, but reversed
        (
            '''In return, my client is offering a salary in the region of £50-£55K and an excellent benefits package.
...
Salary: £50,000.00 to £55,000.00 /year''',
            (50000.0, 55000.0, 'year')
        ),

        # "/hr"
        ('SALARY/RATE AND BENEFITS: £40/hr LTD', (40.0, 40.0, 'hour')),
        # "ph"
        ('travel to customer sites will be required.) Rate: £19 - £25.00ph', (19.0, 25.0, 'hour')),
        # "p/h"
        ('ate ideally with own transport Pay rate starts at £11.00p/h - £13.50p/h + additional hours + Benefits.', (11.0, 13.5, 'hour')),
        # "p.hr"
        ('Basic 39 hours per week with a Pay rate of £11.75 p.hr for days plus overtime premium.', (11.75, 11.75, 'hour')),
        # "per annum"
        ('Job Salary: £36,500-£45,000 per annum (dependent upon experience)', (36500.0, 45000.0, 'year')),
        # "pa"
        ('Salary - From £30K pa but negotiable dependant on experience. Compa', (30000.0, 30000.0, 'year')),
        # "p/a"
        ('or this permanent role is a competitive salary of £28 - £32k p/a, Van, fuel card, PDA, Mobile and Company Bene', (28000.0, 32000.0, 'year')),
        # p.a.
        ('Band 7 £31,696 to £41,787 p.a. + 15% high cost area supplement (maximum £4,045) and lease car', (31696.0, 41787.0, 'year')),
        # pd
        ('Day rate; £300pd - £380pd', (300.0, 380.0, 'day')),

        # multiple periods, year wins
        ('tion Type: Contract Location: West Midlands Rate: £30-40 per hour or £40,000 - £50,000pa', (40000.0, 50000.0, 'year')),
        # multiple periods, hour wins
        ('Salary: Starting from £8.50 Per Hour/ £350.00 Per week', (8.5, 8.5, 'hour')),
        # year + hour, "per year" not specified
        (
            '''Starting Salary: £33,364
36.25 hours per week
£17.70 per hour
''',
            (33364.0, 33364.0, 'year')
        ),

        # bonus
        ('Circa £35,000pa plus bonus of around £5,000 Opportunities for g', (35000.0, 35000.0, 'year')),
        ('SALARY\n£21,000 Basic plus £2,000 Bonus', (21000.0, 21000.0, 'year')),
        # overtime rate
        ('Salary: Basic £29,572\nOvertime rate: £20 per/hour', (29572.0, 29572.0, 'year')),

        # not a bonus, but have bonus nearby
        ('you can expect to earn a competitive salary (to £80k) plus bonus and benefits.', (80000.0, 80000.0, 'year')),
        ('Excellent permanent package available from £85,000 to £125,000 + Bonus', (85000.0, 125000.0, 'year')),

        # unreasonably large (also not a salary)
        ('ven track record on projects with a value between £20,000 and £5,000,000.', None),
        ('In addition, £85m has recently been secured', None),
        ('lients in over 100 countries, turn over more than £3 billion global sales a year, and are expanding ra', None),

        # specified without period, then with period
        ('rate £12.50 plus two x bonus... ...phor bronze or aluminium bronze is ideal\nPayrate: £12.50 per hour', (12.5, 12.5, 'hour')),
        
        # "£x-£y per hour and 5" (misparsed the max as a min)
        (
            '''We are offering 40 hours a week at £30 - £45 per hour and 5 day payment terms. This is initial

In return you will receive the following:
£30 - £45 per hour. (limited or umbrella) 40 hours per week)''',
            (30.0, 45.0, 'hour')
        ),
        # "’" as thousands seperator and badly formatted number
        (
            '''£28’000 - £30’000 + Bonus + 23 Days Holiday + Perkbox + Specialist 

* £28’00 - £30’000 + Bonus + 23 Days Holiday + Perkbox + Specialist ''',
            (28000.00, 30000.00, 'year')
        ),
        # invalid range followed by valid range
        (
            '''The person should have experience in projects £50K to £5M. The initial requirement would be for 10 to 20 hours per month at £50 per hour.

Salary: £50.00 to £55.00 /hour''',
            (50.0, 55.0, 'hour')
        ),

        # non-salary range followed by salary
        (' project budgets ranging between £50,000 and £100,000. ... offering a salary up to £25,000 for the perfect candidate.', (25000.0, 25000.0, 'year')),

        #misc
        ('Hourly Rate: Up to £8.20 P.A.Y.E\nSalary: £8.20 /hour', (8.20, 8.20, 'hour'))
    ]

    for text, expected in test_cases:
        actual = extract_salary(text)
        assert actual == expected, 'Expected: {}, Actual: {} '.format(expected, actual)

