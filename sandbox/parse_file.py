import sys
sys.path.append('../src')

from parse_linkedin import parse_profile
from htmlextract import parse_html
from pprint import pprint

site = 'linkedin'
url = 'https://uk.linkedin.com/pub/chris-moss-ma-assoc-cipd/50/814/5b1'

with open('profile.html', 'r') as inputfile:
    html = inputfile.read()
doc = parse_html(html)

result = parse_profile(site, url, url, doc)
pprint(result)







