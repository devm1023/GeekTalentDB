from phrasematch import clean, stem, tokenize

def normalizedSkill(name):
    """Normalize a string describing a skill.

    """
    if not name:
        return None
    nname = stem(name)
    if not nname:
        return None
    nname.sort()
    return ' '.join(nname)

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
    return name
    
def normalizedTitle(name):
    """Normalize a string describing a job title.

    """
    name = parsedTitle(name)
    if not name:
        return None
    nname = stem(name)
    if not nname:
        return None
    return ' '.join(nname)

def normalizedCompany(name):
    """Normalize a string describing a company.

    """
    if not name:
        return None
    nname = clean(name, keep=',-/&', nospace='\'', removebrackets=True).lower()
    nname = nname.split(',')[0]
    nname = nname.split(' - ')[0]
    nname = nname.split(' / ')[0]
    nname = nname.split(' & ')[0]
    nname = nname.replace(' limited', ' ltd')
    nname = clean(nname)
    if not nname:
        return None
    return nname

def normalizedLocation(name):
    """Normalize a string describing a location.

    """
    return ' '.join(name.lower().split())

