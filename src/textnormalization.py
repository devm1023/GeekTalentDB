import nltk.stem.snowball
import nltk.corpus
import unicodedata
import re

_stopwords_en = set(nltk.corpus.stopwords.words('english'))
_stopwords_nl = set(nltk.corpus.stopwords.words('dutch'))

_stemmer_en = nltk.stem.snowball.SnowballStemmer('english')
_stemmer_nl = nltk.stem.snowball.SnowballStemmer('dutch')

_conf = {
    'en' : {
        'stemmer' : _stemmer_en.stem,

        'skillStemmer' : _stemmer_en.stem,
        'skillStopwords' : _stopwords_en - set(['it', 's', 't']),
        'skillReplace' : [
            ('.net', ' dotnet'),
            ('c++', 'cplusplus'),
            ('c#', 'csharp'),
            ('f#', 'fsharp'),
            ('tcp/ip', 'tcpip'),
            ('co-ordin', 'coordin'),
            ],
        
        'titleStopwords' : _stopwords_en - set(['it', 's', 't']),
        'titleSeparators' : [
            ' at ',
            ' for ',
            ],
        'titlePrefixWords' : set([
            'senior',
            'junior',
            'lead',
            'head',
            'chief',
            'honorary',
            'apprentice',
            'intern',
            'freelance',
        ]),
        'titleSuffixWords' : set([
            'intern',
        ]),
        'titleReplace' : [
            ('.net', ' dotnet'),
            ('c++', 'cplusplus'),
            ('c#', 'csharp'),
            ('f#', 'fsharp'),
            ('tcp/ip', 'tcpip'),
            ('co-ordin', 'coordin'),
        ],

        'sectorStopwords' : _stopwords_en - set(['it', 's', 't']),

        'companyStopwords' : (_stopwords_en - set(['it', 's', 't'])) | \
            set(['limited', 'ltd', 'inc', 'plc', 'uk']),

        
        'instituteRegexReplace' : [
            (r'\bu\.', 'university'),
        ],
        'instituteStopwords' : _stopwords_en - set(['it', 's', 't']),

        'degreeRegexReplace' : [
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
        'degreeStopwords' : (_stopwords_en - set(['it', 's', 't', 'a', 'as'])) \
            | set(['degree', 'hons', 'honours', 'honors', 'first', 'class']),
        
        'subjectStopwords' : _stopwords_en - set(['it', 's', 't']),

        'groupStopwords' : _stopwords_en - set(['it', 's', 't']),
    },

    'nl' : {
        'stemmer' : _stemmer_nl.stem,
        
        'skillStemmer' : _stemmer_en.stem,
        'skillStopwords' : _stopwords_nl,
        'skillReplace' : [
            ('.net', ' dotnet'),
            ('c++', 'cplusplus'),
            ('c#', 'csharp'),
            ('f#', 'fsharp'),
            ('tcp/ip', 'tcpip'),
            ],
        
        'titleStopwords' : _stopwords_nl,
        'titleSeparators' : [
            ' at ',
            ' for ',
            ' and ',
            ' bij ',
            ' en ',
            ],
        'titlePrefixWords' : set(),
        'titleSuffixWords' : set(),
        'titleReplace' : [
            ('.net', ' dotnet'),
            ('c++', 'cplusplus'),
            ('c#', 'csharp'),
            ('f#', 'fsharp'),
            ('tcp/ip', 'tcpip'),
        ],

        'companyStopwords' : _stopwords_nl,

        'instituteRegexReplace' : [],
        'instituteStopwords' : _stopwords_nl,

        'degreeRegexReplace' : [],
        'degreeStopwords' : _stopwords_nl,
        
        'subjectStopwords' : _stopwords_nl,

        'groupStopwords' : _stopwords_nl,
    },
}


def clean(s, keep='', nospace='', lowercase=False, removebrackets=False, 
          stopwords=False, sort=False, removeduplicates=False,
          tokenize=False, regexreplace=[], replace=[], stem=None):
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
        s = [stem(w) for w in s]

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


def makeNrmName(language, name):
    return ':'.join([language, name])

def splitNrmName(nrmName):
    splitname = nrmName.split(':')
    if len(splitname) != 2:
        raise ValueError('Invalid normalized name.')
    return tuple(splitname)

def tokenizedSkill(language, name, removebrackets=False):
    if not name:
        return []
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]
    
    return clean(name,
                 nospace='\'’.',
                 lowercase=True,
                 stopwords=conf['skillStopwords'],
                 removebrackets=removebrackets,
                 tokenize=True,
                 replace=conf['skillReplace'],
                 stem=conf['skillStemmer'])

def normalizedSkill(language, name):
    """Normalize a string describing a skill.

    """
    tokens = tokenizedSkill(language, name, removebrackets=True)
    if not tokens:
        return None
    nname = ' '.join(tokens)
    return makeNrmName(language, nname)

def parsedTitle(language, name):
    """Extract the job title from a LinkedIn profile or experience title.

    """
    if not name:
        return None
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    name = clean(name, keep='&/-,\'', removebrackets=True)
    for separator in conf['titleSeparators']:
        name = name.split(separator)[0]
    name = name.split(' - ')[0]
    name = name.split(' / ')[0]
    name = name.split(',')[0]
    if not name:
        return None
    return name

def _splitTitle(language, words):
    if not words:
        return None, None
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    prefix = []
    suffix = []
    i = 0
    while i < len(words):
        if words[i] in conf['titlePrefixWords']:
            prefix.append(words[i])
            i += 1
        else:
            break
    j = len(words)-1
    while j >= i:
        if words[j] in conf['titleSuffixWords']:
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

def normalizedTitle(language, name):
    """Normalize a string describing a job title.

    """
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    nname = parsedTitle(language, name)
    if not nname:
        return None
    tokens = nname.lower().split()
    prefix, title = _splitTitle(language, tokens)
    if not title:
        return None

    title = clean(title,
                  nospace='\'’.',
                  removebrackets=True,
                  stopwords=conf['titleStopwords'],
                  replace=conf['titleReplace'],
                  stem=conf['stemmer'])
    return makeNrmName(language, title)

def normalizedTitlePrefix(language, name):
    nname = parsedTitle(language, name)
    if not nname:
        return None
    tokens = nname.lower().split()
    prefix, title = _splitTitle(language, tokens)
    return prefix

def normalizedSector(name):
    """Normalize a string describing an industry sector.

    """
    if not name:
        return None
    conf = _conf['en']

    nname = clean(name,
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  stopwords=conf['sectorStopwords'],
                  stem=conf['stemmer'])
    if not nname:
        return None
    return nname

def normalizedCompany(language, name):
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
                  stopwords=conf['companyStopwords'])
    
    nname = nname.split(',')[0]
    nname = nname.split(' - ')[0]
    nname = nname.split(' / ')[0]
    nname = clean(nname, keep='&')
    if not nname:
        return None
    return makeNrmName(language, nname)

def normalizedLocation(name):
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

def normalizedInstitute(language, name):
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
                  stopwords=conf['instituteStopwords'],
                  regexreplace=conf['instituteRegexReplace'],
                  stem=conf['stemmer'],
                  sort=True)
    if not nname:
        return None
    return makeNrmName(language, nname)

def normalizedDegree(language, name):
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
                  stopwords=conf['degreeStopwords'],
                  regexreplace=conf['degreeRegexReplace'],
                  stem=conf['stemmer'])
    if not nname:
        return None
    return makeNrmName(language, nname)

def normalizedSubject(language, name):
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
                  stopwords=conf['subjectStopwords'],
                  stem=conf['stemmer'])
    if not nname:
        return None
    return makeNrmName(language, nname)

def normalizedGroup(language, name):
    if not name:
        return None
    if language not in _conf:
        raise ValueError('Invalid language: '+repr(language))
    conf = _conf[language]

    nname = clean(name,
                  nospace='\'’.',
                  lowercase=True,
                  stopwords=conf['groupStopwords'],
                  stem=conf['stemmer'])
    if not nname:
        return None
    return makeNrmName(language, nname)
