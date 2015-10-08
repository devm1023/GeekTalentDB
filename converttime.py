import sys
from datetime import datetime, timedelta

timestamp0 = datetime(year=1970, month=1, day=1)

def usageAbort():
    print('usage: python3 converttime.py (-s | -m) (YYYY-MM-DD | <timestamp>)')
    exit(1)    

if len(sys.argv) < 3 or sys.argv[1] not in ['-s', '-m']:
    usageAbort()

if sys.argv[1] == '-s':
    unit = 1
else:
    unit = 1000
datestr = sys.argv[2]

dateinput = True
try:
    date = datetime.strptime(sys.argv[2], '%Y-%m-%d')
except ValueError:
    dateinput = False
    try:
        date = int(sys.argv[2])
    except ValueError:
        usageAbort()

if dateinput:
    print(int((date - timestamp0).total_seconds()*unit))
else:
    dt = timedelta(seconds=date/unit)
    print((timestamp0 + dt).strftime('%Y-%m-%d %H:%M:%S'))

