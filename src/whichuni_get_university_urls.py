from htmlextract import parse_html
from pprint import pprint

urls = []
xp_url = '//div[@class="institution-result--header"]/h3/a/@href'

num_pages = 16;
for i in range(num_pages):
    with open('page_{0}.htm'.format((i + 1))) as inputfile:
        content = inputfile.read()
        doc = parse_html(content)
        new_urls = doc.xpath(xp_url)
        new_urls = ['http://university.which.co.uk{0:s}'.format(url) for url in new_urls]
        new_urls_2 = urls + new_urls
        urls = new_urls_2

with open('university_urls.txt', 'w') as outputfile:
    for url in urls:
        outputfile.write('{0}\n'.format(url))


print('done')