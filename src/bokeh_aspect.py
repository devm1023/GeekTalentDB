__all__ = ['set_aspect']

from bokeh.models import Range1d

def set_aspect(fig, x, y, aspect=1, margin=0.1):
    """Set the plot ranges to achieve a given aspect ratio.

    Args:
      fig (bokeh Figure): The figure object to modify.
      x (iterable): The x-coordinates of the displayed data.
      y (iterable): The y-coordinates of the displayed data.
      aspect (float, optional): The desired aspect ratio. Defaults to 1.
        Values larger than 1 mean the plot is squeezed horizontally.
      margin (float, optional): The margin to add for glyphs (as a fraction
        of the total plot range). Defaults to 0.1
    """
    xmin = min(xi for xi in x)
    xmax = max(xi for xi in x)
    ymin = min(yi for yi in y)
    ymax = max(yi for yi in y)
    width = (xmax - xmin)*(1+2*margin)
    if width <= 0:
        width = 1.0
    height = (ymax - ymin)*(1+2*margin)
    if height <= 0:
        height = 1.0
    xcenter = 0.5*(xmax + xmin)
    ycenter = 0.5*(ymax + ymin)
    r = aspect*(fig.plot_width/fig.plot_height)
    if width < r*height:
        width = r*height
    else:
        height = width/r
    fig.x_range = Range1d(xcenter-0.5*width, xcenter+0.5*width)
    fig.y_range = Range1d(ycenter-0.5*height, ycenter+0.5*height)

if __name__ == '__main__':
    from bokeh.plotting import figure, output_file, show

    x = [-1, +1, +1, -1]
    y = [-1, -1, +1, +1]
    output_file("bokeh_aspect.html")
    p = figure(plot_width=400, plot_height=300, tools='pan,wheel_zoom',
               title="Aspect Demo")
    set_aspect(p, x, y, aspect=2)
    p.circle(x, y, size=10)
    show(p)

