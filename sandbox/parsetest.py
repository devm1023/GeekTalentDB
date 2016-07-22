import sys
sys.path.append('../src')

from lxml.etree import tostring
from htmlextract import parse_html, extract, extract_many, \
    get_attr, format_content

# profile xpaths
xp_picture  = ('//*[@id="topcard"]/div[@class="profile-card vcard"]'
               '/div[@class="profile-picture"]/a/img')
xp_name     = '//*[@id="name"]'
xp_title    = '//*[@class="headline title"]/span'
xp_location = '//*[@id="demographics"]/dd[@class="descriptor adr"]/span'
xp_sector   = '//*[@id="demographics"]/dd[@class="descriptor"]/span'
xp_description = '//*[@id="summary"]/div[@class="description"]/p'
xp_connections = '//*[@class="member-connections"]/strong'
xp_skills = '//*[@id="skills"]/ul/li[@class="skill"]'
xp_extraskills = '//*[@id="skills"]/ul/li[@class="skill extra"]'
xp_experiences = ('//*[@id="experience"]/ul[@class="positions"]'
                  '/li[@class="position"]')
xp_educations = '//*[@id="education"]/ul/li'

# skill xpaths
xp_skill_url = 'a'
xp_skill_name = './/span'

# experience xpaths
xp_exp_title = ['header/h4[@class="item-title"]/a/span',
                'header/h4[@class="item-title"]/a']
xp_exp_company_url = 'header/h5[@class="item-subtitle"]/a'
xp_exp_company = ['header/h5[@class="item-subtitle"]/a/span',
                  'header/h5[@class="item-subtitle"]/a',
                  'header/h5[@class="item-subtitle"]']
xp_exp_daterange = 'div[@class="meta"]/span[@class="date-range"]/time'
xp_exp_location = 'div[@class="meta"]/span[@class="location"]'
xp_exp_description = ['div[@class="description"]/p',
                      'p[@class="description"]']

# education xpaths
xp_edu_institute = ['header/*[@class="item-title"]/a/span',
                    'header/*[@class="item-title"]/a',
                    'header/*[@class="item-title"]']
xp_edu_url = 'header/*[@class="item-title"]/a'
xp_edu_course = ['header/*[@class="item-subtitle"]/span',
                 'header/*[@class="item-subtitle"]']
xp_edu_daterange = 'div[@class="meta"]/span[@class="date-range"]/time'
xp_edu_description = ['div[@class="description"]/p',
                      'p[@class="description"]']


def parse_skill(element):
    d = {}
    url = extract(element, xp_skill_url, get_attr('href'))
    d['url'] = url.split('?')[0] if url else None
    d['name'] = extract(element, xp_skill_name, required=True)

    return d


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


def parse_education(element):
    d = {}
    d['institute'] = extract(element, xp_edu_institute)
    url = extract(element, xp_edu_url, get_attr('href'))
    d['url'] = url.split('?')[0] if url else None
    d['course'] = extract(element, xp_edu_course)
    daterange = extract_many(element, xp_edu_daterange)
    d['start'] = d['end'] = None
    if len(daterange) > 0:
        d['start'] = daterange[0]
    if len(daterange) > 1:
        d['end'] = daterange[1]
    if len(daterange) > 2:
        raise ValueError('Too many <time> tags in date range.')
    d['description'] = extract(element, xp_edu_description, format_content)

    return d


def parse_profile(url, redirect_url, timestamp, doc):
    d = {'url' : redirect_url,
         'timestamp' : timestamp}

    # d['picture_url'] = extract(doc, xp_picture, get_attr('data-delayed-url'))
    # d['name'] = extract(doc, xp_name, required=True)
    # d['location'] = extract(doc, xp_location)
    # d['sector'] = extract(doc, xp_sector)
    # d['title'] = extract(doc, xp_title)
    # d['description'] = extract(doc, xp_description, format_content)
    # d['connections'] = extract(doc, xp_connections)

    # d['skills'] = extract_many(doc, xp_skills, parse_skill)
    # d['skills'].extend(extract_many(doc, xp_extraskills, parse_skill))
    d['experiences'] = extract_many(doc, xp_experiences, parse_experience)
    # d['educations'] = extract_many(doc, xp_educations, parse_education)
    
    return d


with open('profile.html', 'r') as inputfile:
    html = inputfile.read()
    
doc = parse_html(html)
profile = parse_profile('', '', None, doc)
# print(profile['name'])
# print()
# for skill in profile['skills']:
#     print(skill['name'])
#     print(skill['url'])
#     print()
for experience in profile['experiences']:
    print(experience['current'])
    print(experience['title'])
    print(experience['company'])
    print(experience['company_url'])
    print(experience['start'])
    print(experience['end'])
    print(experience['location'])
    print(experience['description'])
    print()
# for education in profile['educations']:
#     print(education['institute'])
#     print(education['url'])
#     print(education['course'])
#     print(education['start'])
#     print(education['end'])
#     print(education['description'])
#     print()

print()


