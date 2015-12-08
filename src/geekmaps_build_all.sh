#!/bin/bash

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


echo "Initialising geekmaps database."
echo "# python3 $scriptdir/initdb.py geekmaps"
python3 $scriptdir/initdb.py geekmaps || exit 1
echo
echo
echo "Copying catalogues."
if [ "$3" = "--sudo" ]
then
    echo "# sudo -u postgres pg_dump -t skill -t skill_word -t title -t title_word -t sector -t company -t company_word -t word -a analytics | sudo -u postgres psql geekmaps"
    sudo su -c "sudo -u postgres pg_dump -t skill -t skill_word -t title -t title_word -t sector -t company -t company_word -t word -a analytics | sudo -u postgres psql geekmaps" || exit 1
else
    echo "# pg_dump -t skill -t skill_word -t title -t title_word -t sector -t company -t company_word -t word -a analytics | psql geekmaps"
    pg_dump -t skill -t skill_word -t title -t title_word -t sector -t company -t company_word -t word -a analytics | psql geekmaps || exit 1
fi
echo
echo
echo "Computing NUTS codes."
echo "# python3 $scriptdir/geekmaps_compute_nuts.py $1 $2"
python3 $scriptdir/geekmaps_compute_nuts.py $1 $2 || exit 1
