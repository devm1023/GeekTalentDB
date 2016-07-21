import sys
sys.path.append('../src')

from htmlextract import parse_html, extract, extract_many, \
    get_attr, format_content

# profile xpaths
xp_picture  = ('//*[@id="topcard"]/div[@class="profile-card vcard"]'
               '/div[@class="profile-picture"]/a/img')
xp_name     = '//*[@id="name"]'
xp_title    = '//*[@class="headline title"]/span'
xp_location = '//*[@id="demographics"]/dd[@class="descriptor adr"]/span'
xp_sector   = '//*[@id="demographics"]/dd[@class="descriptor"]/span'
xp_summary  = '//*[@id="summary"]/div[@class="description"]/p'
xp_connections = '//*[@class="member-connections"]/strong'
xp_experiences = ('//*[@id="experience"]/ul[@class="positions"]'
                  '/li[@class="position"]')

# experience xpaths
xp_exp_title = 'header/h4[@class="item-title"]//span'
xp_exp_company_url = 'header/h5[@class="item-subtitle"]/a'
xp_exp_company = 'header/h5[@class="item-subtitle"]//span'
xp_exp_daterange = 'div[@class="meta"]/span[@class="date-range"]/time'
xp_exp_location = 'div[@class="meta"]/span[@class="location"]'
xp_exp_description = 'p[@class="description"]'


def parse_experience(element):
    d = {}
    d['current'] = element.get('data-section') == 'currentPositionsDetails'
    d['title'] = extract(element, xp_exp_title, required=True)
    d['company'] = extract(element, xp_exp_company)
    url = extract(element, xp_exp_company_url, get_attr('href'))
    d['company_url'] = url.split('?')[0] if url else None
    daterange = extract_many(element, xp_exp_daterange)
    d['start'] = d['end'] = None
    if len(daterange) > 0:
        d['start'] = daterange[0]
    if len(daterange) > 1:
        d['end'] = daterange[1]
    if len(daterange) > 2:
        raise ValueError('Too many <time> tags in date range.')
    d['location'] = extract(element, xp_exp_location)
    d['description'] = extract(element, xp_exp_description, format_content)

    return d

def parse_profile(url, redirect_url, timestamp, doc):
    d = {'url' : redirect_url,
         'timestamp' : timestamp}

    d['picture_url'] = extract(doc, xp_picture, get_attr('data-delayed-url'))
    d['name'] = extract(doc, xp_name, required=True)
    d['location'] = extract(doc, xp_location)
    d['sector'] = extract(doc, xp_sector)
    d['title'] = extract(doc, xp_title)
    d['summary'] = extract(doc, xp_summary, format_content)
    d['connections'] = extract(doc, xp_connections)

    d['experiences'] = extract_many(doc, xp_experiences, parse_experience)
    
    return d


for i in range(8, 9):
    with open('profiles/profile_{0:02d}.html'.format(i), 'r') as inputfile:
        html = inputfile.read()    
    doc = parse_html(html)
    profile = parse_profile('', '', None, doc)
    print(i, profile['name'])
    for experience in profile['experiences']:
        print()
        print(experience['current'])
        print(experience['title'])
        print(experience['company'])
        print(experience['company_url'])
        print(experience['start'])
        print(experience['end'])
        print(experience['location'])
        print(experience['description'])
        
    print()


