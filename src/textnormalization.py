import nltk.stem.snowball
import nltk.corpus
import unicodedata
import re

_stopwords_en = set(nltk.corpus.stopwords.words('english')) \
                - set(['it', 's', 't', 'can', 'do', 'm', 'd', 'after'])
_protect_en = set(['hospitality', 'animal', 'animals'])
_stemmer_en = nltk.stem.snowball.SnowballStemmer('english')

_stopwords_nl = set(nltk.corpus.stopwords.words('dutch'))
_protect_nl = set()
_stemmer_nl = nltk.stem.snowball.SnowballStemmer('dutch')


_conf = {
    'en' : {
        'stemmer' : _stemmer_en.stem,
        'protect' : _protect_en,

        'skill_stemmer' : _stemmer_en.stem,
        'skill_protect' : _protect_en,
        'skill_stopwords' : _stopwords_en,
        'skill_replace' : [
            ('.net', ' dotnet'),
            ('c++', 'cplusplus'),
            ('c#', 'csharp'),
            ('f#', 'fsharp'),
            ('tcp/ip', 'tcpip'),
            ('co-ordin', 'coordin'),
            ],

        'title_stopwords' : _stopwords_en,
        'title_separators' : [
            ' at ',
            ' for ',
            ],
        'title_prefix_words' : set([
            'senior',
            'junior',
            'lead',
            'head',
            'chief',
            'honorary',
            'apprentice',
            'intern',
            'freelance',
            'trainee',
            'associate',
            'staff',
            'graduate',
        ]),
        'title_suffix_words' : set([
            'intern',
            'apprentice',
        ]),
        'title_replace' : [
            ('.net', ' dotnet'),
            ('c++', 'cplusplus'),
            ('c#', 'csharp'),
            ('f#', 'fsharp'),
            ('tcp/ip', 'tcpip'),
            ('co-ordin', 'coordin'),
            ('advisor', 'adviser'),
            ('back-end', 'backend'),
            ('back end', 'backend'),
            ('front-end', 'frontend'),
            ('front end', 'frontend'),
            ('ms dynamics', 'microsoft dynamics'),
        ],

        'title_regex_replace' : [
            # 2x title/x2 title/title 2x/title x2
            (r'^[0-9]+ ?[xX]', ''),
            (r'^[xX] ?[0-9]+', ''),
            (r'[0-9]+ ?[xX]$', ''),
            (r'[xX] ?[0-9]+$', ''),
        ],

        'sector_stopwords' : _stopwords_en,

        'company_stopwords' : _stopwords_en | \
            set(['limited', 'ltd', 'inc', 'plc', 'uk']),


        'institute_regex_replace' : [
            (r'\bu\.', 'university'),
        ],
        'institute_stopwords' : _stopwords_en,

        'degree_regex_replace' : [
            (r'\b[0-9]+((st)|(nd)|(rd)|(th))?\b', ''),
            (r'\bb\.?\s*s\.?\s*c\b', 'bachelor of science'),
            (r'\bb\.?\s*a\b', 'bachelor of arts'),
            (r'\bb\.?\s*eng\b', 'bachelor of engineering'),
            (r'\bm\.?\s*s\.?\s*c\b', 'master of science'),
            (r'\bm\.?\s*a\b', 'master of arts'),
            (r'\bm\.?\s*b\.?\s*a\b', 'master of business administration'),
            (r'\bm\.?\s*phil\b', 'master of philosophy'),
            (r'\bph\.?\s*d\b', 'doctor of philosophy'),
        ],
        'degree_stopwords' : (_stopwords_en - set(['a', 'as'])) \
            | set(['degree', 'hons', 'honours', 'honors', 'first', 'class']),

        'subject_stopwords' : _stopwords_en,

        'group_stopwords' : _stopwords_en,
    },

    'nl' : {
        'stemmer' : _stemmer_nl.stem,
        'protect' : _protect_nl,

        'skill_stemmer' : _stemmer_en.stem,
        'skill_protect' : _protect_en,
        'skill_stopwords' : _stopwords_nl,
        'skill_replace' : [
            ('.net', ' dotnet'),
            ('c++', 'cplusplus'),
            ('c#', 'csharp'),
            ('f#', 'fsharp'),
            ('tcp/ip', 'tcpip'),
            ],

        'title_stopwords' : _stopwords_nl,
        'title_separators' : [
            ' at ',
            ' for ',
            ' and ',
            ' bij ',
            ' en ',
            ],
        'title_prefix_words' : set(),
        'title_suffix_words' : set(),
        'title_replace' : [
            ('.net', ' dotnet'),
            ('c++', 'cplusplus'),
            ('c#', 'csharp'),
            ('f#', 'fsharp'),
            ('tcp/ip', 'tcpip'),
        ],
        'title_regex_replace': [],

        'company_stopwords' : _stopwords_nl,

        'institute_regex_replace' : [],
        'institute_stopwords' : _stopwords_nl,

        'degree_regex_replace' : [],
        'degree_stopwords' : _stopwords_nl,

        'subject_stopwords' : _stopwords_nl,

        'group_stopwords' : _stopwords_nl,
    },
}


def clean(s, keep='', nospace='', lowercase=False, removebrackets=False,
          stopwords=False, sort=False, removeduplicates=False,
          tokenize=False, regexreplace=[], replace=[], stem=None,
          protect=set()):
    """Clean up text.

    Note:
      The following operations are performed:

      * accents are removed,
      * unwanted characters (by default everything except letters and digits)
        are removed or replaced by spaces,
      * sequences of whitespace characters are replaced by single spaces,
      * text in brackets is removed (optional),
      * text is converted to lowercase (optional),
      * stopwords are removed (optional),
      * words are sorted alphabetically (optional),
      * duplicate words are removed (optional),
      * text replacements (optional), and
      * stemming (optional).

    Args:
      text (str): The string to clean.
      keep (str, optional): By default all characters which are not letters or
        digits are replaced by spaces. Any additional characters which should
        be retained must be listed in `keep`. Defaults to the empty string.
      nospace (str, optional): Characters listed in `nospace` will be removed
        but *not* replaced by a space character. Defaults to the empty string.
      lowercase (bool, optional): If ``True`` the `text` is converted to
        lowercase. Defaults to ``False``.
      stopwords (list of str or None, optional): List of stopwords to remove.
        Defaults to ``None``, in which case no stopwords are removed.
      removebrackets (bool, optional): If ``True``, any text enclosed in
        parantheses, brackets, or braces is removed. Defaults to ``False``.
      sort (bool, optional): Whether or not the words in `text` should be
        sorted alphabetically. Defaults to ``False``.
      removeduplicates (bool, optional): Whether or not to remove duplicate
        words in text. Only takes effect if `sort` is set to ``True``. Defaults
        to ``False``.
      tokenize (bool, optional): Whether to return a list of words instead of
        a string. Defaults to ``False``.
      regexreplace (list of tuples, optional): List of regular expression text
        replacements to perform after removal of accents and lowercasing. Each
        element of the list must be a tuple of two strings representing the left
        and right-hand side of the regular expression. Defaults to an empty list.
      replace (list of tuples, optional): List of ordinary text replacements to
        perform after removal of accents, lowercasing, and regular expression
        replacement. Each element of the list must be a tuple of two strings
        representing the left and right-hand side of the replacement. Defaults
        to an empty list.
      stem (callable or None, optional): Function to stem the words in `text`.
        Defaults to ``None``, in which case no stemming is performed.
      protect (set of str, optional): Set of words which should not be stemmed.

    """
    # remove accents and lowercase
    if lowercase:
        s = [c.lower() for c in unicodedata.normalize('NFD', s) \
             if unicodedata.category(c) != 'Mn']
    else:
        s = [c for c in unicodedata.normalize('NFD', s) \
             if unicodedata.category(c) != 'Mn']
    s = ''.join(s)

    # make replacements
    for lhs, rhs in regexreplace:
        s = re.sub(lhs, rhs, s)
    for lhs, rhs in replace:
        s = s.replace(lhs, rhs)

    # remove unwanted characters and brackets
    l = []
    plvl = 0
    blvl = 0
    clvl = 0
    for c in s:
        if removebrackets:
            if c == '(':
                plvl += 1
                c = ' '
            elif c == ')':
                plvl -= 1
                c = ' '
            elif c == '[':
                blvl += 1
                c = ' '
            elif c == ']':
                blvl -= 1
                c = ' '
            elif c == '{':
                clvl += 1
                c = ' '
            elif c == '}':
                clvl -= 1
                c = ' '
        oc = ord(c)
        if c in nospace:
            c = ''
        elif not ((oc >= 65 and oc <= 90) or \
                  (oc >= 97 and oc <= 122) or \
                  (oc >= 48 and oc <= 57) or \
                  c in keep):
            c = ' '
        if lowercase:
            c = c.lower()
        if not removebrackets or (plvl <= 0 and blvl <= 0 and clvl <= 0):
            l.append(c)
    s = ''.join(l)

    # remove stopwords
    s = s.split()
    if stopwords:
        stopwords = set(stopwords)
    else:
        stopwords = set()
    if stopwords:
        if lowercase:
            s = [w for w in s if w not in stopwords]
        else:
            s = [w for w in s if w.lower() not in stopwords]

    # do stemming
    if stem is not None:
        s = [(w if w in protect else stem(w)) for w in s]

    # sort and remove duplicates
    if sort:
        if removeduplicates:
            s = list(set(s))
        s.sort()

    # return result
    if tokenize:
        return s
    else:
        return ' '.join(s)


def make_nrm_name(tpe, source, language, name):
    if not tpe or not source or not language or not name:
        return None
    return ':'.join([tpe, source, language, name])

def split_nrm_name(nrm_name):
    splitname = nrm_name.split(':')
    if len(splitname) != 4:
        raise ValueError('Invalid normalized name.')
    return tuple(splitname)

def tokenized_skill(language, name, removebrackets=False):
    if not name:
        return []
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    return clean(name,
                 nospace='\'’.',
                 lowercase=True,
                 stopwords=conf['skill_stopwords'],
                 removebrackets=removebrackets,
                 tokenize=True,
                 replace=conf['skill_replace'],
                 stem=conf['skill_stemmer'],
                 protect=conf['skill_protect'])

def normalized_skill(source, language, name):
    """Normalize a string describing a skill.

    """
    tokens = tokenized_skill(language, name, removebrackets=True)
    if not tokens:
        return None
    nname = ' '.join(tokens)
    return make_nrm_name('skill', source, language, nname)

def parsed_title(language, name):
    """Extract the job title from a LinkedIn profile or experience title.

    """
    if not name:
        return None
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    name = clean(name, keep='&/-,\'+#', removebrackets=True)
    for separator in conf['title_separators']:
        name = name.split(separator)[0]

    # split up to first non-prefix part
    split = re.split(r'- | / |,', name)
    name = []
    for part in split:
        part = part.strip(' /-,')
        if not part:
            continue

        name.append(part)

        if part.lower() not in conf['title_prefix_words']:
            break

    name = ' '.join(name)

    if not name:
        return None
    return name

def _split_title(language, words):
    if not words:
        return None, None
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    prefix = []
    suffix = []
    i = 0
    while i < len(words):
        if words[i] in conf['title_prefix_words']:
            prefix.append(words[i])
            i += 1
        else:
            break
    j = len(words)-1
    while j >= i:
        if words[j] in conf['title_suffix_words']:
            suffix.append(words[j])
            j -= 1
        else:
            break
    main = ' '.join(words[i:j+1])
    prefix = ' '.join(prefix+suffix)
    if not main:
        main = None
    if not prefix:
        prefix = None
    return prefix, main

def normalized_title(source, language, name):
    """Normalize a string describing a job title.

    """
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    nname = parsed_title(language, name)
    if not nname:
        return None
    tokens = nname.lower().split()
    prefix, title = _split_title(language, tokens)
    if not title:
        return None

    title = clean(title,
                  nospace='\'’.',
                  removebrackets=True,
                  stopwords=conf['title_stopwords'],
                  replace=conf['title_replace'],
                  regexreplace=conf['title_regex_replace'],
                  stem=conf['stemmer'],
                  protect=conf['protect'])
    return make_nrm_name('title', source, language, title)

def normalized_title_prefix(language, name):
    nname = parsed_title(language, name)
    if not nname:
        return None
    tokens = nname.lower().split()
    prefix, title = _split_title(language, tokens)
    return prefix

def normalized_sector(name):
    """Normalize a string describing an industry sector.

    """
    if not name:
        return None
    conf = _conf['en']

    nname = clean(name,
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  stopwords=conf['sector_stopwords'],
                  stem=conf['stemmer'],
                  protect=conf['protect'])
    if not nname:
        return None
    return make_nrm_name('sector', 'linkedin', 'en', nname)

def normalized_company(source, language, name):
    """Normalize a string describing a company.

    """
    if not name:
        return None
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    nname = clean(name,
                  keep=',-/&',
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  stopwords=conf['company_stopwords'])

    nname = nname.split(',')[0]
    nname = nname.split(' - ')[0]
    nname = nname.split(' / ')[0]
    nname = clean(nname, keep='&')
    if not nname:
        return None
    return make_nrm_name('company', source, language, nname)

def normalized_location(name):
    """Normalize a string describing a location.

    """
    if not name:
        return None
    nname = ' '.join(name.lower().split())
    nname = nname.replace('en omgeving', '')
    nname = nname.replace('reino unido', 'united kingdom')
    if not nname:
        return None
    return nname

def normalized_adzuna_location(loc0, loc1, loc2, loc3, loc4):
    """Normalize a string describing a location.

    """
    nloc = ', '.join(filter(None, (loc4, loc3, loc2, loc1, loc0))).lower()

    if not nloc:
        return None
    nloc = nloc.replace('en omgeving', '')
    nloc = nloc.replace('reino unido', 'united kingdom')
    if not nloc:
        return None
    return nloc

def normalized_institute(source, language, name):
    """Normalize a string describing an educational institute.

    """
    if not name:
        return None
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    nname = clean(name,
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  stopwords=conf['institute_stopwords'],
                  regexreplace=conf['institute_regex_replace'],
                  stem=conf['stemmer'],
                  protect=conf['protect'],
                  sort=True)
    if not nname:
        return None
    return make_nrm_name('institute', source, language, nname)

def normalized_degree(source, language, name):
    """Normalize a string describing a degree.

    """
    if not name:
        return None
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    nname = name.split(',')[0]
    nname = clean(nname,
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  stopwords=conf['degree_stopwords'],
                  regexreplace=conf['degree_regex_replace'],
                  stem=conf['stemmer'],
                  protect=conf['protect'])
    if not nname:
        return None
    return make_nrm_name('degree', source, language, nname)

def normalized_subject(source, language, name):
    if not name:
        return None
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    nname = name.split('/')[0]
    nname = clean(nname,
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  stopwords=conf['subject_stopwords'],
                  stem=conf['stemmer'],
                  protect=conf['protect'])
    if not nname:
        return None
    return make_nrm_name('subject', source, language, nname)

def normalized_entity(type, source, language, name):
    if type == 'skill':
        return normalized_skill(source, language, name)
    elif type == 'title':
        return normalized_title(source, language, name)
    elif type == 'company':
        return normalized_company(source, language, name)
    elif type == 'sector':
        return normalized_sector(name)
    elif type == 'institute':
        return normalized_institute(source, language, name)
    elif type == 'degree':
        return normalized_degree(source, language, name)
    elif type == 'subject':
        return normalized_subject(source, language, name)
    else:
        raise ValueError('Unknown entity type `{0:s}`.'.format(type))


