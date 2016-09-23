from pprint import pprint
import argparse

def get_new_url_1(old_url):
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

def get_new_url_2(old_url):
    if len(old_url.split('/')) == 8:
        profile_name = old_url.split('/')[4:-3].pop().replace('-', '')
        url_domain = old_url.split('/')[2];
        new_url = 'https://{0}/in/{1}'.format(url_domain, profile_name)
        return new_url
    else:
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('url', help='The URL to generate possible new urls from')
    args = parser.parse_args()
    pprint([args.url, get_new_url_1(args.url), get_new_url_2(args.url)])