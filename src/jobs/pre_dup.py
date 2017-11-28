
import sys
del sys.path[:]
sys.path.append('/Users/aidanas/PycharmProjects/GeekTalentDB/src/jobs')
sys.path.append('/Users/aidanas/PycharmProjects/GeekTalentDB/src')
sys.path.append('/Applications/PyCharm CE.app/Contents/helpers/pydev')
sys.path.append('/Users/aidanas/PycharmProjects/GeekTalentDB')
sys.path.append('/Users/aidanas/PycharmProjects/GeekTalentDB/src')
sys.path.append('/Applications/PyCharm CE.app/Contents/helpers/pydev')
sys.path.append('/Library/Frameworks/Python.framework/Versions/3.5/lib/python35.zip')
sys.path.append('/Library/Frameworks/Python.framework/Versions/3.5/lib/python3.5')
sys.path.append('/Library/Frameworks/Python.framework/Versions/3.5/lib/python3.5/plat-darwin')
sys.path.append('/Library/Frameworks/Python.framework/Versions/3.5/lib/python3.5/lib-dynload')
sys.path.append('/Library/Frameworks/Python.framework/Versions/3.5/lib/python3.5/site-packages')
sys.path.append('/Library/Frameworks/Python.framework/Versions/3.5/lib/python3.5/site-packages/pip-9.0.1-py3.5.egg')

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
    