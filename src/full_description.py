import requests
import re
from lxml import html


def full_description(redirect_url):
    page = requests.get(redirect_url, verify=False, timeout=10)
    tree = html.fromstring(page.content)

    try:
        description = tree.xpath('/html/body/div[1]/div/div[1]/div/div[2]')
    except:
        description = tree.xpath('//*[@id="aplitrak_job_content"]/div[2]')

    raw_html = html.tostring(description[0], pretty_print=True).decode('utf-8')
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)

    return cleantext



