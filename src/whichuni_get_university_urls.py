from htmlextract import parse_html
from pprint import pprint

urls = []
xp_url = '//div[@class="institution-result--header"]/h3/a/@href'

 # save all search pages for universities at 
 # university.which.com and then change num here
 # save from page_1.htm to page_
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