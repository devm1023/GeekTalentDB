#!/bin/bash

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Writing skills catalog to 'skills.csv'."
echo "# python3 $scriptdir/analytics_dump_catalog.py skills $1 skills.csv"
python3 $scriptdir/analytics_dump_catalog.py skills $1 skills.csv || exit 1
echo
echo "Writing titles catalog to 'titles.csv'."
echo "# python3 $scriptdir/analytics_dump_catalog.py titles $1 titles.csv"
python3 $scriptdir/analytics_dump_catalog.py titles $1 titles.csv || exit 1
echo
echo "Writing comapnies catalog to 'companies.csv'."
echo "# python3 $scriptdir/analytics_dump_catalog.py companies $1 companies.csv"
python3 $scriptdir/analytics_dump_catalog.py companies $1 companies.csv || exit 1
echo
echo "Writing institutes catalog to 'institutes.csv'."
echo "# python3 $scriptdir/analytics_dump_catalog.py institutes $1 institutes.csv"
python3 $scriptdir/analytics_dump_catalog.py institutes $1 institutes.csv || exit 1
echo
echo "Writing degrees catalog to 'degrees.csv'."
echo "# python3 $scriptdir/analytics_dump_catalog.py degrees $1 degrees.csv"
python3 $scriptdir/analytics_dump_catalog.py degrees $1 degrees.csv || exit 1
echo
echo "Writing subject catalog to 'subject.csv'."
echo "# python3 $scriptdir/analytics_dump_catalog.py subjects $1 subjects.csv"
python3 $scriptdir/analytics_dump_catalog.py subjects $1 subjects.csv || exit 1
echo
echo "Writing sectors catalog to 'sectors.csv'."
echo "# python3 $scriptdir/analytics_dump_catalog.py sectors $1 sectors.csv"
python3 $scriptdir/analytics_dump_catalog.py sectors $1 sectors.csv || exit 1
