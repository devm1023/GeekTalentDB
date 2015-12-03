#!/bin/bash

echo "Initialising geekmaps database."
python3 initdb.py geekmaps || exit 1
echo
echo
echo "Copying catalogues."
pg_dump -t skill -t skill_word -t title -t title_word -t sector -t company -t company_word -t word -a analytics | psql geekmaps || exit 1
echo
echo
echo "Computing NUTS codes."
python3 geekmaps_compute_nuts.py $1 $2 || exit 1
