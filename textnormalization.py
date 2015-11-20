import nltk.stem.snowball
import nltk.corpus
import unicodedata
import re

_stemmer = nltk.stem.snowball.SnowballStemmer('english')
_stopwords = set([
        'a',
        'an',
        'as',
        'at',
        'the',
        'for',
        'and',
        'or',
        'of',
        'in',
        'to',
        'into',
        'on',
        'by',
        'with',
        'via',
])    

def clean(s, keep='', nospace='', lowercase=False, removebrackets=False, 
          removestopwords=False, sort=False, removeduplicates=False,
          tokenize=False, regexreplace=[], replace=[], stem=False):
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
      removestopwords (bool or list of str, optional): Whether or not to remove
        stopwords. If ``True`` any word contained in nltk's list of english
        stopwords is removed. Alternatively, you can pass a list of (lowercased) 
        stopwords to remove. Defaults to ``False``, in which case no stopwords
        are removed.
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
      stem (bool, optional): Whether to stem the words in `text`. Defaults to
        ``False``.

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
    if removestopwords is True:
        stopwords = _stopwords
    elif hasattr(removestopwords, '__iter__'):
        stopwords = set(removestopwords)
    else:
        stopwords = set()
    if stopwords:
        if lowercase:
            s = [w for w in s if w not in stopwords]
        else:
            s = [w for w in s if w.lower() not in stopwords]

    # do stemming
    if stem:
        s = [_stemmer.stem(w) for w in s]

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


def tokenizedSkill(name, removebrackets=False):
    if not name:
        return []
    replace = [
        ('.net', ' dotnet'),
        ('c++', 'cplusplus'),
        ('c#', 'csharp'),
        ('f#', 'fsharp'),
        ('tcp/ip', 'tcpip'),
        ('co-ordin', 'coordin'),
    ]
    return clean(name,
                 nospace='\'’.',
                 lowercase=True,
                 removestopwords=True,
                 removebrackets=removebrackets,
                 tokenize=True,
                 replace=replace,
                 stem=True)

def normalizedSkill(name):
    """Normalize a string describing a skill.

    """
    tokens = tokenizedSkill(name, removebrackets=True)
    if not tokens:
        return None
    return ' '.join(tokens)

def parsedTitle(name):
    """Extract the job title from a LinkedIn profile or experience title.

    """
    if not name:
        return None
    name = clean(name, keep='&/-,\'', removebrackets=True)
    name = name.split(' - ')[0]
    name = name.split(' / ')[0]
    name = name.split(' at ')[0]
    name = name.split(' for ')[0]
    name = name.split(',')[0]
    if not name:
        return None
    return name

titlePrefixWords = set([
    'senior',
    'junior',
    'lead',
    'head',
    'chief',
    'apprentice',
    'intern',
    'freelance',
])

titleSuffixWords = set([
    'intern',
])

def _splitTitle(stems):
    if not stems:
        return None, None
    prefix = []
    suffix = []
    i = 0
    while i < len(stems):
        if stems[i] in titlePrefixWords:
            prefix.append(stems[i])
            i += 1
        else:
            break
    j = len(stems)-1
    while j >= i:
        if stems[j] in titleSuffixWords:
            suffix.append(stems[j])
            j -= 1
        else:
            break
    main = ' '.join(stems[i:j+1])
    prefix = ' '.join(prefix+suffix)
    if not main:
        main = None
    if not prefix:
        prefix = None
    return prefix, main

def normalizedTitle(name):
    """Normalize a string describing a job title.

    """
    nname = parsedTitle(name)
    if not nname:
        return None
    replace = [
        ('.net', ' dotnet'),
        ('c++', 'cplusplus'),
        ('c#', 'csharp'),
        ('f#', 'fsharp'),
        ('tcp/ip', 'tcpip'),
        ('co-ordin', 'coordin'),
    ]
    tokens = clean(nname,
                   nospace='\'’.',
                   lowercase=True,
                   removebrackets=True,
                   removestopwords=True,
                   replace=replace,
                   tokenize=True)
    prefix, title = _splitTitle(tokens)
    return title

def normalizedTitlePrefix(name):
    nname = parsedTitle(name)
    if not nname:
        return None
    tokens = clean(nname,
                   nospace='\'’.',
                   lowercase=True,
                   removebrackets=True,
                   tokenize=True)
    prefix, title = _splitTitle(tokens)
    return prefix

def normalizedSector(name):
    """Normalize a string describing an industry sector.

    """
    if not name:
        return None
    nname = clean(name,
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  removestopwords=True)
    if not nname:
        return None
    return nname

def normalizedCompany(name):
    """Normalize a string describing a company.

    """
    if not name:
        return None
    stopwords = set(['limited', 'ltd', 'inc', 'plc', 'uk'])
    nname = clean(name,
                  keep=',-/&',
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  removestopwords=stopwords)
    
    nname = nname.split(',')[0]
    nname = nname.split(' - ')[0]
    nname = nname.split(' / ')[0]
    nname = clean(nname, keep='&')
    if not nname:
        return None
    return nname

def normalizedLocation(name):
    """Normalize a string describing a location.

    """
    if not name:
        return None
    nname = ' '.join(name.lower().split())
    if not nname:
        return None
    return nname

def normalizedInstitute(name):
    """Normalize a string describing an educational institute.

    """
    if not name:
        return None
    regexreplace = [
        (r'\bu\.', 'university'),
        ]
    nname = clean(name,
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  removestopwords=True,
                  regexreplace=regexreplace,
                  stem=True)
    if not nname:
        return None
    return nname

def normalizedDegree(name):
    """Normalize a string describing a degree.

    """
    if not name:
        return None
    nname = name.split(',')[0]
    regexreplace = [
        (r'\b[0-9]+((st)|(nd)|(rd)|(th))?\b', ''),
        (r'\bb\.?\s*s\.?\s*c\b', 'bachelor of science'),
        (r'\bb\.?\s*a\b', 'bachelor of arts'),
        (r'\bb\.?\s*eng\b', 'bachelor of engineering'),
        (r'\bm\.?\s*s\.?\s*c\b', 'master of science'),
        (r'\bm\.?\s*a\b', 'master of arts'),
        (r'\bm\.?\s*b\.?\s*a\b', 'master of business administration'),
        (r'\bm\.?\s*phil\b', 'master of philosophy'),
        (r'\bph\.?\s*d\b', 'doctor of philosophy'),
        ]
    stopwords \
        = (_stopwords - set(['a', 'as'])) | \
        set(['degree', 'hons', 'honours', 'honors', 'first', 'class'])
    nname = clean(nname,
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  removestopwords=stopwords,
                  regexreplace=regexreplace,
                  stem=True)
    if not nname:
        return None
    return nname

def normalizedSubject(name):
    if not name:
        return None
    nname = name.split('/')[0]
    nname = clean(nname,
                  nospace='\'’.',
                  lowercase=True,
                  removebrackets=True,
                  removestopwords=True,
                  stem=True)
    if not nname:
        return None
    return nname
