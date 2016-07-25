from lxml.html import fromstring as parse_html


def get_text(element):
    return element.text

def get_stripped_text(element):
    text = element.text
    if text:
        text = text.strip()
    if not text:
        return None
    return text

def _get_attr(attr, element):
    if attr not in element.attrib:
        raise RuntimeError('No attribute {0:s} in {1:s} element' \
                           .format(repr(attr), element.tag))
    return element.get(attr)

def get_attr(attr):
    return lambda element: _get_attr(attr, element)

def _tokenize_string(text):
    if text is None:
        return []
    return text.split()

def _tokenize_element(element):
    tokens = _tokenize_string(element.text)
    for child in element.getchildren():
        if child.tag == 'br':
            tokens.append('\n')
        elif child.tag == 'p':
            if len(tokens) < 1 or tokens[-1] != '\n':
                tokens.extend(['\n', '\n'])
            elif len(tokens) < 2 or tokens[-2] != '\n':
                tokens.append('\n')
        tokens.extend(_tokenize_element(child))
        tokens.extend(_tokenize_string(child.tail))
    return tokens

def format_content(element):
    tokens = _tokenize_element(element)
    tokens = [t for t in tokens if t]
    strings = []
    for token in tokens:
        if strings and not token.isspace() and not strings[-1].isspace():
            strings.append(' ')
        strings.append(token)
    return ''.join(strings).strip()


def _extract(doc, xpaths, f=get_stripped_text, one=False,
             required=False):
    if isinstance(xpaths, str):
        xpaths = [xpaths]
    results = []
    for xpath in xpaths:
        elements = doc.xpath(xpath)
        if one and len(elements) > 1:
            raise RuntimeError('Multiple elements found at {0:s}'.format(xpath))
        results.extend(f(element) for element in elements)
        if one and results:
            return results[0]
    if not results:
        if required:
            raise RuntimeError('No elements found at xpaths {0:s}' \
                               .format(str(xpaths)))
        elif one:
            return None
    return results

def extract(doc, xpath, f=get_stripped_text, required=False):
    return _extract(doc, xpath, f=f, one=True, required=required)

def extract_many(doc, xpath, f=get_stripped_text, required=False):
    return _extract(doc, xpath, f=f, one=False, required=required)
