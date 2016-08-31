from lxml.html import fromstring as parse_html


def get_text(element):
    """Retreive the text of an ``HtmlElement`` object.

    """
    return element.text

def get_stripped_text(element):
    """Retreive the text of an ``HtmlElement`` object stripped of whitespace.

    """
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
    """Return a function that extracts attribute `attr` from an ``HtmlElement``.

    """
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
    """Return the content of `element` with tags removed and basic formatting.

    """
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
    """Extract information from a specific sub-tag in a ``HtmlElement`` tree.

    Args:
      doc (HtmlElement): The parsed HTML code.
      xpath (str): XPATH string which locates the HTML element of interest in
        `doc`. The XPATH should match at most one element in `doc`.
      f (callable, optional): Function to apply to the element located by
        `xpath`. Defaults to ``get_stripped_text``.
      required (bool, optional): Whether to raise an exception when no elements
        match `xpath`. Defaults to ``False``.

    Raises:
      RuntimeError: More than one element matched `xpath` or no element matched
        `xpath` and `required` was ``True``.

    Returns:
      The return value of `f` applied to the element located by `xpath`.

    """
    return _extract(doc, xpath, f=f, one=True, required=required)

def extract_many(doc, xpath, f=get_stripped_text, required=False):
    """Extract information from sub-tags in a ``HtmlElement`` tree.

    Args:
      doc (HtmlElement): The parsed HTML code.
      xpath (str): XPATH string which selects the HTML elements of interest in
        `doc`.
      f (callable, optional): Function to apply to the elements selected by
        `xpath`. Defaults to ``get_stripped_text``.
      required (bool, optional): Whether to raise an exception when no elements
        match `xpath`. Defaults to ``False``.

    Raises:
      RuntimeError: No element matched `xpath` and `required` was ``True``.

    Returns:
      The return values of `f` applied to the elements selected by `xpath`.

    """
    return _extract(doc, xpath, f=f, one=False, required=required)
