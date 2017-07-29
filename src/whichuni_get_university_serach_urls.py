"""
    Builds a file with universities course search urls.
    uses university_urls.txt for input and courses_search_urls_2.txt
    for output.
"""
from htmlextract import parse_html
from pprint import pprint
import requests

xp_url = '//div[contains(@class, "view-all-courses")]/a/@href'
domain_url = 'https://university.which.co.uk'

with open('university_urls.txt', 'r') as infile, open('courses_search_urls_2.txt', 'w') as outfile:
    for url in infile:
        url = url.rstrip('\n')
        try:
            r = requests.get(url)
            print('Got URL {0}'.format(url))
            content = r.text
            doc = parse_html(content)
            elems = doc.xpath(xp_url)
            assert len(elems) == 1
            courses_url = domain_url + doc.xpath(xp_url)[0]
            print('Extracted courses url: {0}'.format(courses_url))
            outfile.write(courses_url)
        except Exception as e:
            print('URL failed: {0}\n'.format(url))
            raise
        except AssertionError as asserr:
            print('Multiple course url elements found: {0}\n'.format(elems))
            raise

print('done')