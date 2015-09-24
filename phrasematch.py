from nltk.stem.snowball import SnowballStemmer
import string
import numpy as np
from itertools import combinations

_stemmer = SnowballStemmer('english')

_particles = set([
    'a',
    'and',
    'or',
    'of',
    'in',
    'by',
    'with',
    ])


def tokenize(s, removebrackets=False):
    """Break text into words, remove puncutation and convert to lowercase.

    Args:
      s (str): The input string.

    Returns:
      list of str: The list of words.

    """
    s = s.strip().lower()
    s = s.replace("'", '')
    s = s.replace('.net', 'dotnet')
    s = s.replace('c++', 'cplusplus')
    s = s.replace('c#', 'csharp')
    s = s.replace('f#', 'fsharp')
    s = s.replace('tcp/ip', 'tcpip')
    s = s.replace('co-ordin', 'coordin')

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
        if not ((ord(c) >= 97 and ord(c) <= 122) or \
                (ord(c) >= 48 and ord(c) <= 57)):
            c = ' '
        if not removebrackets or (plvl <= 0 and blvl <= 0 and clvl <= 0):
            l.append(c)
            
    s = ''.join(l)
    stems = [stem for stem in s.split() if stem not in _particles]
    return stems


def stem(s, removebrackets=False):
    if s is None:
        return []
    return list(map(_stemmer.stem, tokenize(s, removebrackets=removebrackets)))


def _ratio(nmatch, ntext, nphrase, pw):
    return (nmatch/ntext)**pw * (nmatch/nphrase)


def matchPhrases(phrases, texts, proximity_weight=0.25):
    """Determine whether phrases appear in a list of texts.

    Args:
      phrases (list of str): The phrases to look for.
      texts (list of str): The texts to search.
      proximity_weight (float, optional): A positive number (typically less
        than 1) indicating how the proximity of matching words is weighted.

    Returns:
      numpy array: An array of shape ``(len(phrases), len(texts))`` holding
        numbers between 0 and 1 indicating the quality of the match of each
        phrase with each text. If a phrase consists of `n` words and only `m`
        of them appear in `text` the match score is at most ``m/n``. The match
        score is reduced if the words are far apart. The full formula is::

          max((m/w)**proximity_weight * m/n)

        where `m` is the number of matching words in a "window" of `w`
        consecutive words from the text. The maxumum is taken over all possible
        window sizes and positions.
        
    """
    pw = proximity_weight
    phrasestems = list(map(stem, phrases))
    textstems = list(map(stem, texts))
    result = np.zeros((len(phrases), len(texts)))
    for iphrase, phrasetokens in enumerate(phrasestems):
        for itext, texttokens in enumerate(textstems):
            nphrase = len(phrasetokens)
            ntext = len(texttokens)
            if nphrase <= 0 or ntext <= 0:
                continue
            matches = np.zeros((ntext, nphrase), dtype=np.bool)
            for textpos, texttoken in enumerate(texttokens):
                for phrasepos, phrasetoken in enumerate(phrasetokens):
                    if phrasetoken == texttoken:
                        matches[textpos, phrasepos] = True
                        break

            maxmatch = np.count_nonzero(np.logical_or.reduce(matches))
            maxratio = _ratio(maxmatch, ntext, nphrase, pw)
            for width in range(1, ntext):
                if _ratio(maxmatch, width, nphrase, pw) <= maxratio:
                    break
                for offset in range(ntext-width+1):
                    nmatch = np.count_nonzero(
                        np.logical_or.reduce(matches[offset:offset+width]))
                    ratio = _ratio(nmatch, width, nphrase, pw)
                    maxratio = max(ratio, maxratio)
                    if nmatch >= width:
                        break
            result[iphrase, itext] = maxratio

    return result


def matchPhrase(phrase, text):
    return matchPhrases([phrase], [text])[0,0]


if __name__ == '__main__':
    phrase = 'Python development'
    texts = [
        'developing python programs, and stuff',
        'Development of Python programs',
        'developing programs in python',
        'I keep a python snake as a pet and I come from a developing country.',
    ]
    print('matching "'+phrase+'" in')
    for text in texts:
        print('"'+text+'":', matchPhrases([phrase], [text])[0,0])

    if matchPhrase('', 'blah blah'):
        print('ERROR!!')
    if matchPhrase('blah blah', ''):
        print('ERROR!!')
    if matchPhrase(None, 'blah blah'):
        print('ERROR!!')        
    if matchPhrase('blah blah', None):
        print('ERROR!!')
    if matchPhrase(None, None):
        print('ERROR!!')
        

