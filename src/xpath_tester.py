from htmlextract import parse_html
import requests

# url = 'https://university.which.co.uk/manchester-metropolitan-university-m40'
# url = 'http://university.which.co.uk/search/institution?i%5Bpage%5D=1'
# url = 'http://university.which.co.uk/a-level-explorer/afrikaans/bengali'
url = 'http://university.which.co.uk/university-of-st-andrews-s36'

# xp_url = '//div[@class="institution appContainer"]'
# xp_url = '//div[@class="institution-result--header"]/h3/a/@href'
# xp_url = '//li[contains(@class, "result-card")]'
xp_url = '//div[contains(@class, "view-all-courses")]/a/@href'
# xp_url = '/html/body/main/div[1]/div[4]/div/div[1]/div[2]/a'

try:
    r = requests.get(url)
    print('Got URL {0}'.format(url))
    content = r.text
    doc = parse_html(content)
    elems = doc.xpath(xp_url)
    print('Elements extracted:\n')
    for e in elems:
        print(e)

except Exception as e:
    print('URL failed: {0}\n'.format(url))
    raise

print('\nFinished!')
