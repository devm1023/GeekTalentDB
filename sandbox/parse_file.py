import sys
sys.path.append('../src')

from parse_linkedin import parse_profile
from htmlextract import parse_html
from pprint import pprint

site = 'linkedin'
url = 'https://www.linkedin.com/pub/laura-keilty/99/b3/a4a?trk=pub-pbmap'

with open('profile.html', 'r') as inputfile:
    html = inputfile.read()
doc = parse_html(html)

result = parse_profile(site, url, url, doc)
pprint(result)







