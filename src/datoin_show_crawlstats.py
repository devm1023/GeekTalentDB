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
            results.append(None)
            continue
        elif n == 1:
            results.append(GVar(x, 0))
            continue

        mean = x/n
        var = max(x2/(n-1) - (n+1)/(n-1)*mean**2, 0)
        results.append(GVar(mean, sqrt(var/n)))
    return results

def toplegend(top=1, ncol=1, pad=0.02):
    plt.subplots_adjust(top=top)
    plt.legend(ncol=ncol, loc='lower left', mode='expand',
               bbox_to_anchor=(0, 1+pad, 1, 1),
               frameon=True,
               borderaxespad=0)


def make_linkedin_plots(obj, pdf):
    def savefig(xlabel, ylabel, rotate=False):
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
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
    print(lidata['indexed_on'].data)
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
    plt.legend(loc='lower right')
    savefig('date', 'LinkedIn profiles', rotate=True)

    delay_hist.plot(drawstyle='steps')
    savefig('delay [days]', 'LinkedIn profiles', rotate=True)

    crawlcounts = lidata['newcrawl'] + lidata['recrawl']
    crawlcounts.plot(xconvert=lambda x: from_date + x)
    savefig('date', 'crawled LinkedIn profiles')

    histogram_collections = [
        [('nexp', 'nexp2', 'experiences'),
         ('nedu', 'nedu2', 'educations'),
         ('ncat', 'ncat2', 'skills'),
         ('picture_url', 'picture_url', 'picture'),
         ('title', 'title', 'title'),
         ('description', 'description', 'description'),
        ],
        [('url', 'url', 'URL'),
         ('name', 'name', 'name'),
         ('first_name', 'first_name', 'first name'),
         ('last_name', 'last_name', 'last name'),
         ('city', 'city', 'city'),
         ('country', 'country', 'country'),
        ]
    ]
    for histograms in histogram_collections:
        for xname, x2name, ylabel in histograms:
            hist = Histogram1D(
                like=lidata[xname],
                data=get_averages(zip(lidata[xname].data,
                                      lidata[x2name].data,
                                      crawlcounts.data)))
            mean = sum(y for y in hist.data if y is not None).mean \
                   /sum(1 for y in hist.data if y is not None)
            hist /= mean
            hist.plot(xconvert=lambda x: from_date + x, errorbars=True,
                      label=ylabel)
        plt.gca().set_ylim(bottom=0)
        toplegend(top=0.85, ncol=3)
        savefig('date', 'normalised frequency', rotate=True)
    
    histograms = [
        ('nexp', 'nexp2', 'average number of experiences'),
        ('nedu', 'nedu2', 'average number of educations'),
        ('ncat', 'ncat2', 'average number of skills'),
        ('url', 'url', 'fraction of profiles with URL'),
        ('picture_url', 'picture_url', 'fraction of profiles with picture'),
        ('name', 'name', 'fraction of profiles with name'),
        ('first_name', 'first_name', 'fraction of profiles with first name'),
        ('last_name', 'last_name', 'fraction of profiles with last name'),
        ('city', 'city', 'fraction of profiles with city'),
        ('country', 'country', 'fraction of profiles with country'),
        ('title', 'title', 'fraction of profiles with title'),
        ('description', 'description', 'fraction of profiles with description'),
    ]
    for xname, x2name, ylabel in histograms:
        hist = Histogram1D(
            like=lidata[xname],
            data=get_averages(zip(lidata[xname].data,
                                  lidata[x2name].data,
                                  crawlcounts.data)))
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




