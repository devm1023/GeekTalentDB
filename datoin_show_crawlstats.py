import matplotlib.pyplot as plt
import pickle
import sys

filename = sys.argv[1]
with open(filename, 'rb') as pclfile:
    obj = pickle.load(pclfile)

fromDate = obj['fromDate']
indexHist = obj['indexHist']
crawlHist = obj['crawlHist']
delayHist = obj['delayHist']
    
crawlHist.plot(xconvert=lambda x: fromDate + x, drawstyle='steps')
plt.xlabel('crawl date')
plt.show()

delayHist.plot(drawstyle='steps')
plt.xlabel('delay [days]')
plt.show()

indexHist.plot(xconvert=lambda x: fromDate + x, drawstyle='steps')
plt.xlabel('index date')
plt.show()
