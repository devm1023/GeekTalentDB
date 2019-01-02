import requests
import argparse

"""
   Script to obtain a number of top skills from solr.
   Example query url:
   http://52.19.175.216:8983/solr/aphrodite/select
   ?q=content_type+%3A+main_profile&
   fq=merged_sector+%3A+(%22Machinery+OR+Industrial+Automation%22+OR+%22Machines%22+OR
   +%22Mechanical+or+Industrial+Engineering%22+OR+%22Electrical%2Felectronic+manufacturing%22+OR
   +%22Consumer+Electronics%22+OR+%22Semiconductors%22)&rows=0&wt=json&indent=true
   &facet=true&facet.field=skills&facet.limit=2000
"""

SOLR_HOST = 'http://130.211.99.149:8983/solr/'
SOLR_CORE = 'aphrodite'
SOLR_REQ = '{0}{1}/select?q=content_type+%3A+main_profile&fq=merged_sector+%3A+({2})+&rows=0&wt=json' \
           '&indent=true&facet=true&facet.field=skills&facet.limit={3}'

def main(args):

    sector = args.sector
    limit = args.limit

    if sector == 'eng':
        sector = '"Machinery" OR "Industrial Automation" OR "Machines" OR ' \
                 '"Mechanical or Industrial Engineering" OR "Electrical/Electronic manufacturing" OR ' \
                 '"Consumer Electronics" OR "Semiconductors"'
    elif sector == 'healthcare':
        sector = '"Biotechnology" OR "Hospital & Health Care" OR "Medical Practice" OR "Mental Health Care" OR ' \
                 '"Pharmaceuticals"'
    elif sector == 'construction':
        sector = '"construction"'
    elif sector ==  'finance':
        sector = '"Capital Markets" OR "Financial Services" OR "Insurance" OR "Investment Banking" OR ' \
                 '"Investment Management" OR "Venture Capital & Private Equity"'
    elif sector == 'legal':
        sector = '"Alternative Dispute Resolution" OR "Judiciary" OR "Law Practice" OR "Legal Services" OR' \
                 '"Legislative Office"'
    elif sector == 'hr':
        sector = '"Human Resources" OR "Staffing and Recruiting"'
    elif sector == 'goods':
        sector = '"Consumer Goods" OR "Consumer Services" OR "Apparel & Fashion" OR "Cosmetics" OR "Furniture" OR' \
                ' "Luxury Goods & Jewelry" OR "Real Estate" OR "Retail" OR "Sporting Goods" OR "Supermarkets" OR ' \
                ' "Tobacco" OR "Wholesale" OR "Wines and Spirits"'
    elif sector == 'logistics':
        sector = '"Logistics and Supply Chain" OR "Maritime" OR "Package/Freight Delivery" OR' \
                 '"Transportation/Trucking/Railroad" OR "Warehousing"'
    elif sector == 'manufacturing':
        sector = '"Chemicals" OR "Food Production" OR "Glass, Ceramics & Concrete" OR "Mining & Metals" OR ' \
                 '"Nanotechnology" OR "Packaging and Containers" OR Paper & Forest Products" OR "Plastics" OR ' \
                 '"Railroad Manufacture" OR "Textiles"'

    #TODO Previous cluster sectors required to be update along with new sectors to be processed - should match mappings file

    url = SOLR_REQ.format(SOLR_HOST, SOLR_CORE, sector, limit)

    print('Querying SOLR with: {0}\n'.format(url))

    try:
        r = requests.get(url)
        json = r.json()
        skills = json['facet_counts']['facet_fields']['skills']

    except Exception as e:
        print('URL failed: {0}\n'.format(url))
        raise

    with open('solr_skills.txt', 'w') as outputfile:
        for i, skill in enumerate(skills):
            if i % 2 == 1:
                continue
            outputfile.write('{0}\n'.format(skill))

    print('\nDone!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--sector', choices=['it', 'eng', 'healthcare', 'finance', 'construction', 'manufacturing', 'retail', 'hr', 'legal', 'logistics', 'goods'], default='it',
                        help='Merged sector to get skill from.')
    parser.add_argument('--limit', type=int, default=1000,
                        help='Number of skills to be obtained.')
    args = parser.parse_args()
    main(args)
