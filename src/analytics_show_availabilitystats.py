from histograms import Histogram1D, Histogram2D
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn
from math import log, sqrt, exp
import pickle
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('inputfile',
                    help='The name of the input file')
parser.add_argument('outputfile',
                    help='The name of the output file',
                    nargs='?')
args = parser.parse_args()

with open(args.inputfile, 'rb') as pclfile:
    hist = pickle.load(pclfile)

def drawHist(hist, title=None):
    if isinstance(hist, Histogram1D):
        hist.errorbar(drawstyle='steps')
        if title is not None:
            plt.title(title)
        plt.xlabel('employment duration [years]')
        plt.ylabel('annual retention rate')
        plt.ylim(0.0, 1.0)
    elif isinstance(hist, list):
        for  i, (label, h) in enumerate(hist):
            n = len(hist)
            offset = (i-n/2)*0.2
            h.errorbar(drawstyle='steps', label=label, offset=offset)
        if title is not None:
            plt.title(title)
        plt.xlabel('employment duration [years]')
        plt.ylabel('annual retention rate')
        plt.ylim(0.0, 1.0)
        plt.legend(loc='lower left')
    else:
        raise ValueError('Invalid `hist` argument.')

def show(pdf):
    if pdf is None:
        plt.show()
    else:
        pdf.savefig()
        plt.close()

pdf = None
if args.outputfile:
    pdf = PdfPages(args.outputfile)

        
drawHist(hist['duration'])
show(pdf)

drawHist(hist['nexp'],
         title='Retention rate by number of previous employments')
show(pdf)

drawHist(hist['age'],
         title='Retention rate by time since first employment')
show(pdf)

drawHist(hist['maxdur'],
         title='Retention rate by max duration of previous employments')
show(pdf)

drawHist(hist['prevdur'],
         title='Retention rate by duration of previous employment')
show(pdf)

imax = max((t for t in hist['title'] if t[1].data[0] is not None),
           key=lambda x: x[1].data[0].mean)
imin = min((t for t in hist['title'] if t[1].data[0] is not None),
           key=lambda x: x[1].data[0].mean)
drawHist([imin, imax],
         title='Retention rate by job title')
show(pdf)

if pdf is not None:
    pdf.close()
