import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pickle
import sys
import seaborn

inputfilename = sys.argv[1]
outputfilename = None
if len(sys.argv) > 2:
    outputfilename = sys.argv[2]

with open(inputfilename, 'rb') as pclfile:
    obj = pickle.load(pclfile)

fromDate = obj['fromDate']
indexHist = obj['indexHist']
crawlHist = obj['crawlHist']
delayHist = obj['delayHist']

pdf = None
if outputfilename:
    pdf = PdfPages(outputfilename)

indexHist.plot(xconvert=lambda x: fromDate + x, drawstyle='steps',
               label='indexed')
crawlHist.plot(xconvert=lambda x: fromDate + x, drawstyle='steps',
               label='crawled')
plt.xlabel('date')
plt.ylabel('LinkedIn profiles')
plt.legend(loc='lower right')
if pdf:
    pdf.savefig()
    plt.close()
else:
    plt.show()

delayHist.plot(drawstyle='steps')
plt.xlabel('delay [days]')
plt.ylabel('LinkedIn profiles')
if pdf:
    pdf.savefig()
    plt.close()
else:
    plt.show()


if pdf:
    pdf.close()
