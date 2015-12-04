#!/bin/bash

echo "# python3 initdb.py analytics"
python3 initdb.py analytics
echo
echo
echo "# python3 analytics_build_catalogs.py $1 $2"
python3 analytics_build_catalogs.py $1 $2 || exit 1
echo
echo
echo "# python3 analytics_build_liprofiles.py $1 $2"
python3 analytics_build_liprofiles.py $1 $2 || exit 1
echo
echo
echo "# python3 analytics_build_wordlists.py $1 $2"
python3 analytics_build_wordlists.py $1 $2 || exit 1

