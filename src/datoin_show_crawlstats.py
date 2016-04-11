import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pickle
import argparse
import seaborn
from histograms import cumulated_histogram, Histogram1D, GVar
from math import sqrt

deep_colors = list(seaborn.color_palette('deep'))
pastel_colors = list(seaborn.color_palette('pastel'))


def get_averages(iterable):
    results = []
    for elem in iterable:
        x = elem[0]
        x2 = elem[1]
        n = sum(elem[2:])
        if n == 0:
            results.append(GVar(0))
            continue
        elif n == 1:
            results.append(GVar(x, x))
            continue

        mean = x/n
        var = max(x2/(n-1) - (n+1)/(n-1)*mean**2, 0)
        results.append(GVar(mean, sqrt(var/n)))
    return results
        

def make_linkedin_plots(obj, pdf):
    def savefig(xlabel, ylabel, rotate=False):
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.legend(loc='lower right')
        if rotate:
            plt.xticks(rotation=20)
            plt.subplots_adjust(bottom=0.15)
        if pdf:
            pdf.savefig()
            plt.close()
        else:
            plt.show()
        
    from_date = obj['from_date']
    lidata = obj['linkedin']
    index_hist = cumulated_histogram(
        lidata['indexed_on'], const=lidata['indexed_before'])
    newcrawl_hist = cumulated_histogram(
        lidata['newcrawl'], const=lidata['newcrawl_before'])
    recrawl_hist = cumulated_histogram(
        lidata['recrawl'], const=lidata['recrawl_before'])
    failedcrawl_hist = cumulated_histogram(
        lidata['failedcrawl'], const=lidata['failedcrawl_before'])
    delay_hist = lidata['delay']

    (newcrawl_hist + recrawl_hist + failedcrawl_hist) \
        .plot(xconvert=lambda x: from_date + x,
              facecolor=pastel_colors[2], color=deep_colors[2],
              label='failed')
    (newcrawl_hist + recrawl_hist) \
        .plot(xconvert=lambda x: from_date + x,
              facecolor=pastel_colors[1], color=deep_colors[1],
              label='recrawl')
    (newcrawl_hist + recrawl_hist) \
        .plot(xconvert=lambda x: from_date + x,
              facecolor=pastel_colors[0], color=deep_colors[0],
              label='new')
    index_hist.plot(xconvert=lambda x: from_date + x, color='k',
                   label='indexed')
    savefig('date', 'LinkedIn profiles', rotate=True)

    delay_hist.plot(drawstyle='steps')
    savefig('delay [days]', 'LinkedIn profiles')

    for xname, x2name, ylabel in \
        [('nexp', 'nexp2', 'Average number of experiences'),
         ('nedu', 'nedu2', 'Average number of educations'),
         ('ncat', 'ncat2', 'Average number of skills'),
         ('url', 'url', 'Fraction of profiles with URL'),
         ('picture_url', 'picture_url', 'Fraction of profiles with picture'),
         ('name', 'name', 'Fraction of profiles with name'),
         ('first_name', 'first_name', 'Fraction of profiles with first name'),
         ('last_name', 'last_name', 'Fraction of profiles with last name'),
         ('city', 'city', 'Fraction of profiles with city'),
         ('country', 'country', 'Fraction of profiles with country'),
         ('title', 'title', 'Fraction of profiles with title'),
         ('description', 'description',
          'Fraction of profiles with description'),
        ]:
        hist = Histogram1D(
            like=lidata[xname],
            data=get_averages(zip(lidata[xname].data,
                                  lidata[x2name].data,
                                  lidata['newcrawl'].data,
                                  lidata['recrawl'].data)))
        hist.plot(xconvert=lambda x: from_date + x, errorbars=True)
        savefig('date', ylabel, rotate=True)    
    

def main(args):
    inputfilename = args.input_file
    outputfilename = args.output_file

    with open(inputfilename, 'rb') as pclfile:
        obj = pickle.load(pclfile)

    try:
        pdf = None
        if outputfilename:
            pdf = PdfPages(outputfilename)

        make_linkedin_plots(obj, pdf)
    finally:
        if pdf:
            pdf.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file',
                        help='Pickle file generated '
                        'by datoin_get_crawlstats.py')
    parser.add_argument('output_file', nargs='?', default=None,
                        help='PDF file for output. If omitted, plots are shown '
                        'on the screen.')
    args = parser.parse_args()
    main(args)




