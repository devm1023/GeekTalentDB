from linkedin_possible_urls import get_old_url
import canonicaldb as cn
import parsedb as ps
from canonical_import_parse import import_liprofiles

with cn.CanonicalDB() as cndb, ps.ParseDB() as psdb, open('urls.csv', 'r') as inputfile:
    for line in inputfile:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        old_urls = get_old_url(line)
        if len(old_urls):
            profile_id = cndb.query(cn.LIProfile.id) \
                .filter(cn.LIProfile.url.in_(old_urls)) \
                .first()
            if profile_id is not None:
                print('deleting linkedin profile with id {0:d}'.format(profile_id[0]))
                row = cndb.query(cn.LIProfile) \
                    .filter(cn.LIProfile.id == profile_id) \
                    .first()
                cndb.delete(row)
                cndb.commit()
        parse_profile_id = psdb.query(ps.LIProfile.id) \
                               .filter(ps.LIProfile.url == line) \
                               .first()
        if parse_profile_id is not None:
            print('importing parsed record with parsedb id {0:d} into canonical'.format(parse_profile_id[0]))
            import_liprofiles(1, parse_profile_id[0], parse_profile_id[0] + 1, None, None)