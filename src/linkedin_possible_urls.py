import argparse
from pprint import pprint

def has_numbers(string):
    return any(char.isdigit() for char in string)

def has_letters(string):
    return any(char.isalpha() for char in string)

def get_new_url(old_url):
    if len(old_url.split('/')) == 8:
        profile_name = old_url.split('/')[4:-3].pop()
        first_url_segment = old_url.split('/')[5:-2].pop()
        second_url_segment = old_url.split('/')[6:-1].pop()
        last_url_segment = old_url.split('/')[7:].pop().split('?')[0]
        url_domain = old_url.split('/')[2];
        if first_url_segment == '0':
            first_url_segment = ''
        while len(second_url_segment) != 3:
            second_url_segment = '0{0}'.format(second_url_segment)
        while len(last_url_segment) != 3:
            last_url_segment = '0{0}'.format(last_url_segment)
        new_url = 'https://{0}/in/{1}-{2}{3}{4}'.format(url_domain, profile_name, last_url_segment, second_url_segment, first_url_segment)
        return new_url
    else:
        return None

def get_old_url(new_url):
    try:
        if has_numbers(new_url):
            segments = new_url.split('-')[-1];
            name = '-'.join(new_url.split('/')[-1].split('-')[:-1])
            url = '/'.join(new_url.split('/')[:-2])
            if has_numbers(segments[0:3]) and not has_letters(segments[0:3]):
                last_url_segment = str(int(segments[0:3]))
            else:
                last_url_segment = segments[0:3]
            if has_numbers(segments[3:6]) and not has_letters(segments[3:6]):
                second_url_segment = str(int(segments[3:6]))
            else:
                second_url_segment = segments[3:6]
            if has_numbers(segments[6:8]) and not has_letters(segments[6:8]):
                first_url_segment = str(int(segments[6:8]))
            else:
                first_url_segment = segments[6:8]
            new_url = '{0}/pub/{1}/{2}/{3}/{4}'.format(url, name, first_url_segment, second_url_segment, last_url_segment)
            possible_urls = [
                new_url,
                new_url.replace('https', 'http')
            ]
            return possible_urls
        else:
            return []
    except:
        return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('url', help='The URL to generate possible new urls from')
    args = parser.parse_args()
    pprint(get_old_url(args.url))