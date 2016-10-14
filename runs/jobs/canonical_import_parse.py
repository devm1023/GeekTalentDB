
import sys
del sys.path[:]
sys.path.append('/home/geektalent/GeekTalentDB/runs/jobs')
sys.path.append('/home/geektalent/GeekTalentDB/src')
sys.path.append('/usr/local/lib/python3.4/dist-packages/pip-8.1.1-py3.4.egg')
sys.path.append('/usr/lib/python3.4')
sys.path.append('/usr/lib/python3.4/plat-x86_64-linux-gnu')
sys.path.append('/usr/lib/python3.4/lib-dynload')
sys.path.append('/usr/local/lib/python3.4/dist-packages')
sys.path.append('/usr/lib/python3/dist-packages')

from cloudpickle import dump, load

fin = open(sys.argv[1], 'rb')
f = load(fin)
fin.close()

fin = open(sys.argv[2], 'rb')
xvals = load(fin)
fin.close()

fvals = [f(*x) for x in xvals]

fout = open(sys.argv[3], 'wb')
xvals = dump(fvals, fout)
fout.close()
    