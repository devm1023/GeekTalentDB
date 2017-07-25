from htmlextract import parse_html
from pprint import pprint
import requests

urls = []
xp_url = '//div[@class="institution-result--header"]/h3/a/@href'

num_pages = 16
for i in range(num_pages):
    try:
        r = requests.get('http://university.which.co.uk/search/institution?i%5Bpage%5D={0}'.format(i + 1))
        print('Got URL {0}'.format('http://university.which.co.uk/search/institution?i%5Bpage%5D={0}'.format(i + 1)))
        content = r.text
        doc = parse_html(content)
        new_urls = doc.xpath(xp_url)
        new_urls = ['http://university.which.co.uk{0:s}'.format(url) for url in new_urls]
        new_urls_2 = urls + new_urls
        urls = new_urls_2
    except Exception as e:
        print('URL failed: {0}\n{1}\n'.format('http://university.which.co.uk/search/institution?i%5Bpage%5D={0}'.format(i + 1), str(e)))
        raise

with open('university_urls.txt', 'w') as outputfile:
    for url in urls:
        outputfile.write('{0}\n'.format(url))


print('done')