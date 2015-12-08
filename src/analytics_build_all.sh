#!/bin/bash

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "# python3 $scriptdir/initdb.py analytics"
python3 $scriptdir/initdb.py analytics
echo
echo
echo "# python3 $scriptdir/analytics_build_catalogs.py $1 $2"
python3 $scriptdir/analytics_build_catalogs.py $1 $2 || exit 1
echo
echo
echo "# python3 $scriptdir/analytics_build_liprofiles.py $1 $2"
python3 $scriptdir/analytics_build_liprofiles.py $1 $2 || exit 1
echo
echo
echo "# python3 $scriptdir/analytics_build_wordlists.py $1 $2"
python3 $scriptdir/analytics_build_wordlists.py $1 $2 || exit 1

