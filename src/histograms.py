import numpy
import bisect
import matplotlib.pyplot as plt
from math import sqrt
import copy
import itertools


class GVar:
    """Represents a gaussian random variate with a mean and standard deviation.

    Args:
      m (float or any object with `mean` and `sdev` attributes): If `m` is a
        float it specifies the mean of the gaussian variate. Otherwise the
        `mean` member of m is taken to be the mean.
      sdev (float or None, optional): The standard deviation of the gaussian
        variate. If ``None`` the standard deviation is taken from ``m.sdev`` (if
        `m` has that attribute) or set to zero. Defaults to ``None``.

    Attributes:
      mean (float): mean of the gaussian variate.
      sdev (float): standard deviation of the gaussian variate.

    Note:
      `GVar` objects support addition, subtraction, multiplication and division
      with floats or other `GVar` objects. In the latter case errors are combined
      in quadrature.

    """

    def __init__(self, m, sdev=None):
        if hasattr(m, 'mean') and hasattr(m, 'sdev'):
            self.mean = m.mean
            self.sdev = sdev if sdev is not None else m.sdev
        else:
            self.mean = float(m)
            self.sdev = abs(float(sdev)) if sdev is not None else 0.0

    def __add__(self, g):
        if hasattr(g, 'mean') and hasattr(g, 'sdev'):
            return GVar(self.mean + g.mean, sqrt(self.sdev**2 + g.sdev**2))
        else:
            return GVar(self.mean + g, self.sdev)

    def __radd__(self, g):
        if hasattr(g, 'mean') and hasattr(g, 'sdev'):
            return GVar(self.mean + g.mean, sqrt(self.sdev**2 + g.sdev**2))
        else:
            return GVar(self.mean + g, self.sdev)

    def __sub__(self, g):
        if hasattr(g, 'mean') and hasattr(g, 'sdev'):
            return GVar(self.mean - g.mean, sqrt(self.sdev**2 + g.sdev**2))
        else:
            return GVar(self.mean - g.mean, self.sdev)

    def __rsub__(self, g):
        if hasattr(g, 'mean') and hasattr(g, 'sdev'):
            return GVar(g.mean - self.mean, sqrt(self.sdev**2 + g.sdev**2))
        else:
            return GVar(g.mean - self.mean, self.sdev)

    def __mul__(self, a):
        if hasattr(a, 'mean') and hasattr(a, 'sdev'):
            return GVar(self.mean*a.mean,
                        sqrt((self.mean*a.sdev)**2 + (self.sdev*a.mean)**2))
        else:
            return GVar(self.mean*a, self.sdev*a)

    def __rmul__(self, a):
        if hasattr(a, 'mean') and hasattr(a, 'sdev'):
            return GVar(self.mean*a.mean,
                        sqrt((self.mean*a.sdev)**2 + (self.sdev*a.mean)**2))
        else:
            return GVar(self.mean*a, self.sdev*a)

    def __div__(self, a):
        if hasattr(a, 'mean') and hasattr(a, 'sdev'):
            return GVar(self.mean/a.mean,
                        sqrt((self.sdev/a.mean)**2 +
                             (self.mean*a.sdev/a.mean**2)**2))
        else:
            return GVar(self.mean/a, self.sdev/a)

    def __rdiv__(self, a):
        if hasattr(a, 'mean') and hasattr(a, 'sdev'):
            return GVar(a.mean/self.mean,
                        sqrt((a.sdev/self.mean)**2 +
                             (a.mean*self.sdev/self.mean**2)**2))
        else:
            return GVar(a/self.mean, a*self.sdev/self.mean**2)

    def __pow__(self, a):
        return GVar(self.mean**a, abs(a*self.mean**(a-1)*self.sdev))

    def __repr__(self):
        return 'GVar('+repr(self.mean)+', '+repr(self.sdev)+')'

    def __str__(self):
        """Return string representation of ``self``.

        The representation is designed to show at least
        one digit of the mean and two digits of the standard deviation.
        For cases where mean and standard deviation are not
        too different in magnitude, the representation is of the
        form ``'mean(sdev)'``. When this is not possible, the string
        has the form ``'mean +- sdev'``.

        """
        # taken from gvar.GVar in gvar module (lsqfit distribution)
        def ndec(x, offset=2):
            ans = offset - numpy.log10(x)
            ans = int(ans)
            if ans > 0 and x * 10. ** ans >= [0.5, 9.5, 99.5][offset]:
                ans -= 1
            return 0 if ans < 0 else ans
        dv = abs(self.sdev)
        v = self.mean

        # special cases
        if dv == float('inf'):
            return '%g +- inf' % v
        elif v == 0 and (dv >= 1e5 or dv < 1e-4):
            if dv == 0:
                return '0(0)'
            else:
                ans = ("%.1e" % dv).split('e')
                return "0.0(" + ans[0] + ")e" + ans[1]
        elif v == 0:
            if dv >= 9.95:
                return '0(%.0f)' % dv
            elif dv >= 0.995:
                return '0.0(%.1f)' % dv
            else:
                ndecimal = ndec(dv)
                return '%.*f(%.0f)' % (ndecimal, v, dv * 10. ** ndecimal)
        elif dv == 0:
            ans = ('%g' % v).split('e')
            if len(ans) == 2:
                return ans[0] + "(0)e" + ans[1]
            else:
                return ans[0] + "(0)"
        elif dv < 1e-6 * abs(v) or dv > 1e4 * abs(v):
            return '%g +- %.2g' % (v, dv)
        elif abs(v) >= 1e6 or abs(v) < 1e-5:
            # exponential notation for large |self.mean|
            exponent = numpy.floor(numpy.log10(abs(v)))
            fac = 10.**exponent
            mantissa = str(self/fac)
            exponent = "e" + ("%.0e" % fac).split("e")[-1]
            return mantissa + exponent

        # normal cases
        if dv >= 9.95:
            if abs(v) >= 9.5:
                return '%.0f(%.0f)' % (v, dv)
            else:
                ndecimal = ndec(abs(v), offset=1)
                return '%.*f(%.*f)' % (ndecimal, v, ndecimal, dv)
        if dv >= 0.995:
            if abs(v) >= 0.95:
                return '%.1f(%.1f)' % (v, dv)
            else:
                ndecimal = ndec(abs(v), offset=1)
                return '%.*f(%.*f)' % (ndecimal, v, ndecimal, dv)
        else:
            ndecimal = max(ndec(abs(v), offset=1), ndec(dv))
            return '%.*f(%.0f)' % (ndecimal, v, dv * 10. ** ndecimal)


class Histogram2D:
    """Represents two-dimensional histograms.

    Args:
      xvals (list or None, optional): list of central x-values of the histogram
        bins. Derived from `xbins` if ``None`. Defaults to ``None``.
      yvals (list or None, optional): list of central y-values of the histogram
        bins. Derived from `ybins` if ``None``. Defaults to ``None``.
      xbins (list or None, optional): list of bin boundaries in x direction.
        Derived from `xvals` if ``None``. Defaults to ``None``.
      ybins (list or None, optional): list of bin boundaries in y direction.
        Derived from `yvals` if ``None``. Defaults to ``None``.
      data (2D array of objects or None, optional): data members (z-values) of
        the histogram. The first index corresponds to the y-axis.
        If set to ``None`` a suitably dimensioned array of ``None`` values
        is created. Defaults to ``None``.
      like (Histogram2D or None, optional): If not ``None`` the central values
        and bin boundaries of another Histogram2D instance are used. This
        supercedes any values specified for `xvals`, `yvals`, `xbins`, or
        `ybins`. The ``data`` member is not copied. Defaults to ``None``.
      init: Initial value for all bins of the histogram. Defaults to ``None``.

    Attributes:
      xvals (numpy array): list of central x-values of the histogram bins.
      yvals (numpy array): list of central y-values of the histogram bins.
      xbins (numpy array): list of bin boundaries in x direction.
      ybins (numpy array): list of bin boundaries in y direction.
      data (numpy array): data members (z-values) of the histogram.
        The first index corresponds to the x-axis.

    Note:
      Histogram2D supports element-wise addition, subtraction, multiplication,
      and division with other Histogram2D instances as long as their
      `xvals`, `yvals`, `xbins` and `ybins` members are exactly equal. Addition,
      subtraction, multiplication or division by a constant value for bins
      is also supported.

    """
    def __init__(self, xvals=None, yvals=None, xbins=None, ybins=None,
                 data=None, like=None, init=None):
        if like is not None:
            xvals = numpy.array(like.xvals)
            yvals = numpy.array(like.yvals)
            xbins = numpy.array(like.xbins)
            ybins = numpy.array(like.ybins)
        else:
            if xvals is None and xbins is None:
                raise TypeError('Either `like`, `x` or `xbins` must be set.')
            elif xvals is None:
                if not hasattr(xbins, '__len__'):
                    raise ValueError('Argument `xbins` must be iterable.')
                elif len(xbins) < 2:
                    raise ValueError('Argument `xbins` must have at least 2 entries.')
                xbins = numpy.array(xbins)
                xvals = 0.5*(xbins[1:] + xbins[:-1])
            else:
                if not hasattr(xvals, '__len__'):
                    raise ValueError('Argument `xvals` must be iterable.')
                elif len(xvals) < 2 and xbins is None:
                    raise ValueError('Argument `xvals` must have at least 2 entries or xbins must be given.')
                elif len(xvals) < 1:
                    ValueError('Argument `xvals` must have at least 1 entry.')
                xvals = numpy.array(xvals)
                if xbins is None:
                    xbins = 0.5*(xvals[1:] + xvals[:-1])
                    xbins = numpy.array([xvals[0]-(xbins[0]-xvals[0])] + \
                                            list(xbins) + \
                                            [xvals[-1]+(xvals[-1]-xbins[-1])])
                else:
                    if len(xbins) != len(xvals)+1:
                        raise ValueError('Argument `xbins` has wrong length.')
                    for i, x in enumerate(xvals):
                        if x < xbins[i]:
                            raise ValueError('Bin centre outside of bin.')
                    if xvals[-1] > xbins[-1]:
                            raise ValueError('Bin centre outside of bin.')

            if yvals is None and ybins is None:
                raise TypeError('Either `like`, `yvals` or `ybins` must be set.')
            elif yvals is None:
                if not hasattr(ybins, '__len__'):
                    raise ValueError('Argument `ybins` must be iterable.')
                elif len(ybins) < 2:
                    raise ValueError('Argument `ybins` must have at least 2 entries.')
                ybins = numpy.array(ybins)
                yvals = 0.5*(ybins[1:] + ybins[:-1])
            else:
                if not hasattr(yvals, '__len__'):
                    raise ValueError('Argument `yvals` must be iterable.')
                elif len(yvals) < 2 and ybins is None:
                    raise ValueError('Argument `yvals` must have at least 2 entries or `ybins` must be given.')
                elif len(yvals) < 1:
                    raise ValueError('Argument `yvals` must have at least 1 entry.')
                yvals = numpy.array(yvals)
                if ybins is None:
                    ybins = 0.5*(yvals[1:] + yvals[:-1])
                    ybins = numpy.array([yvals[0]-(ybins[0]-yvals[0])] + \
                                            list(ybins) + \
                                            [yvals[-1]+(yvals[-1]-ybins[-1])])
                else:
                    if len(ybins) != len(yvals)+1:
                        raise ValueError('Argument `xbins` has wrong length.')
                    for i, y in enumerate(yvals):
                        if y < ybins[i]:
                            raise ValueError('Bin centre outside of bin.')
                    if yvals[-1] > ybins[-1]:
                            raise ValueError('Bin centre outside of bin.')

        self.xvals = xvals
        self.yvals = yvals
        self.xbins = xbins
        self.ybins = ybins

        if data is None:
            self.data = numpy.full((len(yvals), len(xvals)), init, dtype=object)
        else:
            if not hasattr(data, 'shape') or len(data.shape) != 2:
                raise TypeError('`data` argument must be 2D-array.')
            if len(yvals) != data.shape[0] or len(xvals) != data.shape[1]:
                raise ValueError('`data` argument has wrong dimensions.')
            self.data = data.copy()

    def copy(self):
        """Return an independent copy of self.

        """
        return Histogram2D(like=self, data=self.data)

    def _getxbin(self, x):
        if x < self.xbins[0] or x > self.xbins[-1]:
            raise KeyError('Invalid x-key `'+repr(x)+'`.')
        return max(bisect.bisect_left(self.xbins, x) - 1, 0)
        
    def _getybin(self, y):
        if y < self.ybins[0] or y > self.ybins[-1]:
            raise KeyError('Invalid y-key `'+repr(y)+'`.')
        return max(bisect.bisect_left(self.ybins, y) - 1, 0)
    
    def _getbin(self, x, y):
        return self._getybin(y), self._getxbin(x)

    def __getitem__(self, xy):
        """Return the data value of a specific bin

        Args:
          xy (tuple): A pair of coordinates.

        Returns:
          The data value of the bin which contains the coordinates `xy`.

        Raises:
          KeyError: The coordinates `xy` are outside the range of the histogram.

        """
        x, y = xy
        return self.data[self._getbin(x, y)]
        
    def __setitem__(self, xy, val):
        """Set the data value of a specific bin

        Args:
          xy (tuple): A pair of coordinates.
          val: A data value. The value of the bin containing the coordinates
            `xy` will be set to `val`.

        Raises:
          KeyError: The coordinates `xy` are outside the range of the histogram.

        """
        x, y = xy
        self.data[self._getbin(x, y)] = val

    def inc(self, x, y, by=1):
        try:
            self[x, y] += by
        except KeyError:
            pass

    def center(self, x, y):
        """Return the center of the bin to which (x,y) belongs.

        """
        py, px = self._getbin(x, y)
        return (self.xvals[px], self.yvals[py])        

    def lb(self, x, y):
        """Return the lower left corner of the bin to which (x,y) belongs.

        """
        py, px = self._getbin(x, y)
        return (self.xbins[px], self.ybins[py])

    def ub(self, x, y):
        """Return the upper right corner of the bin to which (x,y) belongs.

        """
        py, px = self._getbin(x, y)
        return (self.xbins[px+1], self.ybins[py+1])
        
    def __iter__(self):
        """Iterate over the bin centres.

        Yields:
          x (float): The x-coordinate of the bin centre.
          y (float): The y-coordinate of the bin centre.

        """
        for y in self.yvals:
            for x in self.xvals:
                yield (x, y)
    
    def iteritems(self):
        """Iterate over all elements of the histogram.

        Yields:
          tuple: ``((x, y), v)`` where ``(x, y)`` is the bin center and `v` the
            value associated with that bin.
        
        """
        for iy, y in enumerate(self.yvals):
            for ix, x in enumerate(self.xvals):
                yield (x, y, self.data[ix, iy])

    def max(self):
        """Return the largest data member in the histogram.

        Note:
          `None` values are ignored.

        """
        result = None
        for x, y, v in self.iteritems():
            if v is not None:
                result = v if result is None else max(v, result)
        return result

    def min(self):
        """Return the smallst data member in the histogram.

        Note:
          `None` values are ignored.

        """
        result = None
        for x, y, v in self.iteritems():
            if v is not None:
                result = v if result is None else min(v, result)
        return result
        
    def sum(self):
        """Return the sum of all data members in the histogram.

        Note:
          `None` values are ignored.

        """
        result = None
        for x, y, v in self.iteritems():
            if v is not None:
                result = v if result is None else v + result
        return result    
        
    def set(self, h):
        """Set all data values of the histogram.

        Args:
          h (Histogram2D or object): If `h` is a Histogram2D instance the
            function iterates over all bins of `h` and sets the corresponding
            bins of `self` to their values. Otherwise all bin values of
            `self` are set to `h`.

        """
        if isinstance(h, Histogram2D):
            for x, y, v in h.iteritems():
                self[x, y] = v
        else:
            self.data.fill(h)

    def settomin(self, h):
        """Set all bins to the minimum of their current value and h.

        Args:
          h (Histogram2D or object): If `h` is a Histogram2D instance the
            function iterates over all bins of `h` and sets the corresponding
            bins of `self` to the minimum of the two values. Otherwise all bins
            of `self` are set to the minimum of their current value and `h`.

        """
        if isinstance(h, Histogram2D):
            for x, y, v in h.iteritems():
                p = self._getbin(x,y)
                s = self.data[p]
                self.data[p] = min(s, v) if s is not None else v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                self.data[p] = min(v, h) if v is not None else h

    def settomax(self, h):
        """Set all bins to the maximum of their current value and h.

        Args:
          h (Histogram2D or object): If `h` is a Histogram2D instance the
            function iterates over all bins of `h` and sets the corresponding
            bins of `self` to the maximum of the two values. Otherwise all bins
            of `self` are set to the maximum of their current value and `h`.

        """
        if isinstance(h, Histogram2D):
            for x, y, v in h.iteritems():
                p = self._getbin(x,y)
                s = self.data[p]
                self.data[p] = max(s, v) if s is not None else v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                self.data[p] = max(v, h) if v is not None else h

    def xslices(self):
        slices = []
        for i in range(len(self.xvals)):
            slices.append(Histogram1D(xvals=self.yvals,
                                      xbins=self.ybins,
                                      data=self.data[:,i]))
        return slices

    def xslice(self, x):
        px = self._getxbin(x)
        return Histogram1D(xvals=self.yvals,
                           xbins=self.ybins,
                           data=self.data[:,px])

    def yslices(self):
        slices = []
        for i in range(len(self.yvals)):
            slices.append(Histogram1D(xvals=self.xvals,
                                      xbins=self.xbins,
                                      data=self.data[i,:]))
        return slices
        
    def yslice(self, x):
        py = self._getybin(y)
        return Histogram1D(xvals=self.xvals,
                           xbins=self.xbins,
                           data=self.data[py,:])

    def contour(self, axes=None, convert=None, **kwargs):
        """Draw contour lines with matplotlib

        Args:
          axes (matplotlib.axes.Axes or None, optonal): Axes object to plot to.
            Uses ``matplotlib.pyplot.gca()`` if ``None``. Defaults to ``None``.
          convert (callable or None, optional): function that converts the
            elements of self.data to floats. If ``None``, the elements of
            ``self.data`` must be floats. Defaults to ``None``.
          **kwargs: all other keyword arguments are passed to
            ``matplotlib.axes.contour``.

        """
        if axes is None:
            axes = plt.gca()
        if convert is None:
            data = self.data
        else:
            data = numpy.vectorize(convert)(self.data)
        return axes.contour(self.xvals, self.yvals, data, **kwargs)

    def contourf(self, axes=None, convert=None, **kwargs):
        """Draw filled contours with matplotlib

        Args:
          axes (matplotlib.axes.Axes or None, optional): Axes object to plot to.
            Uses ``matplotlib.pyplot.gca()`` if None. Defaults to None.
          convert (callable or None, optional): function that converts the
            elements of ``self.data`` to floats. If ``None``, the elements of
            ``self.data`` must be floats. Defaults to ``None``.
          **kwargs: all other keyword arguments are passed to
            ``matplotlib.axes.contourf``.

        """
        if axes is None:
            axes = plt.gca()
        if convert is None:
            data = self.data
        else:
            data = numpy.vectorize(convert)(self.data)
        return axes.contourf(self.xvals, self.yvals, data, **kwargs)

    def pcolormesh(self, axes=None, convert=None, **kwargs):
        """Draw a colored mesh with matplotlib

        Args:
          axes (matplotlib.axes.Axes or None, optional): Axes object to plot to.
            Uses ``matplotlib.pyplot.gca()`` if ``None``. Defaults to ``None``.
          convert (callable or None, optional): function that converts the
            elements of ``self.data`` to floats. If ``None``, the elements of
            ``self.data`` must be floats. Defaults to ``None``.
          **kwargs: all other keyword arguments are passed to
            ``matplotlib.axes.pcolormesh``.

        """
        if axes is None:
            axes = plt.gca()
        if convert is None:
            data = numpy.asarray(self.data, dtype=float)
        else:
            data = numpy.asarray(numpy.vectorize(convert)(self.data),
                                 dtype=float)
        return axes.pcolormesh(self.xbins, self.ybins, data, **kwargs)

    def setxlim(self, axes=None):
        """Set x limits of the current plot.

        Args:
          axes (matplotlib.axes.Axes or None): Axes object whose limits are
            to be set. Uses ``matplotlib.pyplot.gca()`` if ``None``. Defaults to
            ``None``.

        """
        if axes is None:
            axes = plt.gca()
        return axes.set_xlim((self.xbins[0], self.xbins[-1]))

    def setylim(self, axes=None):
        """Set y limits of the current plot.

        Args:
          axes (matplotlib.axes.Axes or None): Axes object whose limits are
            to be set. Uses ``matplotlib.pyplot.gca()`` if ``None``. Defaults to
            ``None``.

        """
        if axes is None:
            axes = plt.gca()
        return axes.set_ylim((self.ybins[0], self.ybins[-1]))
    
    def __iadd__(self, h):
        if isinstance(h, Histogram2D):
            if not (numpy.array_equal(h.xbins, self.xbins) and \
                    numpy.array_equal(h.ybins, self.ybins)):
                raise ValueError('cannot add histograms with different bins')
            for p, v in numpy.ndenumerate(h.data):
                if self.data[p] is not None:
                    self.data[p] += v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                if v is not None:
                    self.data[p] += h
        return self

    def __isub__(self, h):
        if isinstance(h, Histogram2D):
            if not (numpy.array_equal(h.xbins, self.xbins) and \
                    numpy.array_equal(h.ybins, self.ybins)):
                raise ValueError('cannot add histograms with different bins')
            for p, v in numpy.ndenumerate(h.data):
                if self.data[p] is not None:
                    self.data[p] -= v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                if v is not None:
                    self.data[p] -= h
        return self

    def __imul__(self, h):
        if isinstance(h, Histogram2D):
            if not (numpy.array_equal(h.xbins, self.xbins) and \
                    numpy.array_equal(h.ybins, self.ybins)):
                raise ValueError('cannot add histograms with different bins')
            for p, v in numpy.ndenumerate(h.data):
                if self.data[p] is not None:
                    self.data[p] *= v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                if v is not None:
                    self.data[p] *= h
        return self

    def __itruediv__(self, h):
        if isinstance(h, Histogram2D):
            if not (numpy.array_equal(h.xbins, self.xbins) and \
                    numpy.array_equal(h.ybins, self.ybins)):
                raise ValueError('cannot add histograms with different bins')
            for p, v in numpy.ndenumerate(h.data):
                if self.data[p] is not None:
                    self.data[p] = self.data[p]/v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                if v is not None:
                    self.data[p] = self.data[p]/h
        return self

    def __add__(self, h):
        result = self.copy()
        result += h
        return result

    def __sub__(self, h):
        result = self.copy()
        result -= h
        return result

    def __mul__(self, h):
        result = self.copy()
        result *= h
        return result

    def __truediv__(self, h):
        result = self.copy()
        result /= h
        return result

    def __radd__(self, h):
        result = self.copy()
        result += h
        return result

    def __rsub__(self, h):
        result = self.copy()
        result *= -1
        result += h
        return result

    def __rmul__(self, h):
        result = self.copy()
        result *= h
        return result

    def __rtruediv__(self, h):
        result = Histogram2D(like=self)
        for p, v in numpy.ndenumerate(self.data):
            result.data[p] = h/v
        return result


class Histogram1D:
    """Represents one-dimensional histograms.

    Args:
      xvals (list or None, optional): list of central x-values of the histogram
        bins. Derived from `xbins` if ``None``. Defaults to ``None``.
      xbins (list or None, optional): list of bin boundaries in x direction.
        Derived from `xvals` if ``None``. Defaults to ``None``.
      data (array of objects or None, optional): data members (z-values) of the
        histogram. If set to ``None`` a suitably dimensioned array of
        ``None`` values is created. Defaults to ``None``.
      like (Histogram1D or None, optional): If not ``None`` the central values
        and bin boundaries of another Histogram1D instance are used. This
        supercedes any values specified for `xvals` or `xbins`. The
        ``data`` member is not copied.
      init: Initial value for all bins of the histogram. Defaults to ``None``.

    Attributes:
      xvals (numpy array): list of central x-values of the histogram bins.
      xbins (numpy array): list of bin boundaries in x direction.
      data (numpy array): data members (z-values) of the histogram.

    Note:
      Histogram1D supports element-wise addition, subtraction, multiplication,
      and division with other Histogram1D instances as long as their
      `xvals`, `yvals`, `xbins` and `ybins` members are exactly equal. Addition,
      subtraction, multiplication or division by a constant value for bins
      is also supported.

    """
    def __init__(self, xvals=None, xbins=None, data=None, like=None, init=None):
        if like is not None:
            xvals = numpy.array(like.xvals)
            xbins = numpy.array(like.xbins)
        else:
            if xvals is None and xbins is None:
                raise TypeError('Either `like`, `xvals` or `xbins` must be set.')
            elif xvals is None:
                if not hasattr(xbins, '__len__'):
                    raise ValueError('Argument `xbins` must be iterable.')
                elif len(xbins) < 2:
                    raise ValueError('Argument `xbins` must have at least 2 entries.')
                xbins = numpy.array(xbins)
                xbins = numpy.array(xbins)
                xvals = 0.5*(xbins[1:] + xbins[:-1])
            else:
                if not hasattr(xvals, '__len__'):
                    raise ValueError('Argument `xvals` must be iterable.')
                elif len(xvals) < 2 and xbins is None:
                    raise ValueError('Argument `xvals` must have at least 2 entries or `xbins` must be given.')
                elif len(xvals) < 1:
                    raise ValueError('Argument `xvals` must have at least 1 entry.')
                xvals = numpy.array(xvals)
                if xbins is None:
                    xbins = 0.5*(xvals[1:] + xvals[:-1])
                    xbins = numpy.array([xvals[0]-(xbins[0]-xvals[0])] + \
                                            list(xbins) + \
                                            [xvals[-1]+(xvals[-1]-xbins[-1])])
                else:
                    xbins = numpy.array(xbins)
                    if len(xbins) != len(xvals)+1:
                        raise ValueError('Argument `xbins` has wrong length.')
                    for i, x in enumerate(xvals):
                        if x < xbins[i]:
                            raise ValueError('Bin centre outside of bin.')
                    if xvals[-1] > xbins[-1]:
                            raise ValueError('Bin centre outside of bin.')

        self.xvals = xvals
        self.xbins = xbins

        if data is None:
            self.data = numpy.full((len(xvals),), init, dtype=object)
        else:
            if not hasattr(data, '__len__'):
                raise TypeError('`data` argument must be 1D-array.')
            if len(xvals) != len(data):
                raise ValueError('`data` argument has wrong dimension.')
            self.data = numpy.array(data)

    def copy(self):
        """Return an independent copy of self.

        """
        return Histogram1D(like=self, data=self.data)

    def _getbin(self, x):
        if x < self.xbins[0] or x > self.xbins[-1]:
            raise KeyError('Invalid key `'+repr(x)+'`.')
        return max(bisect.bisect_left(self.xbins, x) - 1, 0)

    def __getitem__(self, x):
        """Return the data value of a specific bin

        Args:
          x (float): An x coordinate.

        Returns:
          The data value of the bin which contains the coordinate `x`.

        Raises:
          KeyError: The coordinate `x` is outside the range of the histogram.

        """
        return self.data[self._getbin(x)]
        
    def __setitem__(self, x, val):
        """Set the data value of a specific bin

        Args:
          x (tuple): An x coordinate.
          val: A data value. The value of the bin containing the coordinate
            `x` will be set to `val`.

        Raises:
          KeyError: The coordinate `x` is outside the range of the histogram.

        """
        self.data[self._getbin(x)] = val

    def inc(self, x, by=1):
        try:
            self[x] += by
        except KeyError:
            pass

    def center(self, x):
        """Return the center of the bin to which x belongs.

        """
        return self.xvals[self._getbin(x)]

    def lb(self, x):
        """Return the lower bound of the bin to which x belongs.

        """
        return self.xbins[self._getbin(x)]

    def ub(self, x):
        """Return the upper bound of the bin to which x belongs.

        """
        return self.xbins[self._getbin(x)+1]
        
    def __iter__(self):
        """Iterate over the bin centres.

        Yields:
          x (float): The x-coordinate of the bin centre.

        """
        for x in self.xvals:
            yield x
    
    def iteritems(self):
        """Iterate over all elements of the histogram.

        Yields:
          tuple: ``(x, v)`` where `x` is the bin center and `v` the value
            associated with that bin.
        
        """
        for ix, x in enumerate(self.xvals):
            yield (x, self.data[ix])

    def max(self):
        """Return the largest data member in the histogram.

        Note:
          `None` values are ignored.

        """
        result = None
        for x, v in self.iteritems():
            if v is not None:
                result = v if result is None else max(v, result)
        return result

    def min(self):
        """Return the smallest data member in the histogram.

        Note:
          `None` values are ignored.

        """
        result = None
        for x, v in self.iteritems():
            if v is not None:
                result = v if result is None else min(v, result)
        return result

    def sum(self):
        """Return the sum of all data members in the histogram.

        Note:
          `None` values are ignored.

        """
        result = None
        for x, v in self.iteritems():
            if v is not None:
                result = v if result is None else v + result
        return result    
        
    def set(self, h):
        """Set all data values of the histogram.

        Args:
          h (Histogram1D or object): If `h` is a Histogram1D instance the
            function iterates over all bins of `h` and sets the corresponding
            bins of `self` to their values. Otherwise all bin values of
            `self` are set to `h`.

        """
        if isinstance(h, Histogram1D):
            for x, v in h.iteritems():
                self[x] = v
        else:
            self.data.fill(h)

    def settomin(self, h):
        """Set all bins to the minimum of their current value and h.

        Args:
          h (Histogram1D or object): If `h` is a Histogram1D instance the
            function iterates over all bins of `h` and sets the corresponding
            bins of `self` to the minimum of the two values. Otherwise all bins
            of `self` are set to the minimum of their current value and `h`.

        """
        if isinstance(h, Histogram1D):
            for x, v in h.iteritems():
                p = self._getbin(x)
                s = self.data[p]
                self.data[p] = min(s, v) if s is not None else v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                self.data[p] = min(v, h) if v is not None else h

    def settomax(self, h):
        """Set all bins to the maximum of their current value and h.

        Args:
          h (Histogram1D or object): If `h` is a Histogram1D instance the
            function iterates over all bins of `h` and sets the corresponding
            bins of `self` to the maximum of the two values. Otherwise all bins
            of `self` are set to the maximum of their current value and `h`.

        """
        if isinstance(h, Histogram1D):
            for x, v in h.iteritems():
                p = self._getbin(x)
                s = self.data[p]
                self.data[p] = max(s, v) if s is not None else v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                self.data[p] = max(v, h) if v is not None else h

    def plot(self, *args, **kwargs):
        """Plot the histogram with matplotlib.

        Args:
          *args: all positional arguments are passed to matplotlib.axes.Axes.plot
          axes (matplotlib.axes.Axes or None, optional): Axes object to plot to.
            Uses ``matplotlib.pyplot.gca()`` if ``None``. Defaults to ``None``.
          convert (callable or None, optional): function that converts the
            elements of ``self.data`` to floats. If ``None``, the elements of
            ``self.data`` must be floats or have the attribute ``mean``.
          xconvert (callable or None, optional): function that converts the
            x-values. Defaults to ``None``, in which case no conversion is
            performed.
          **kwargs: all other keyword arguments are passed to
            ``matplotlib.axes.Axes.plot``.

        """
        axes = kwargs.get('axes', None)
        convert = kwargs.get('convert', None)
        xconvert = kwargs.get('xconvert', None)
        if axes is None:
            axes = plt.gca()
        if convert is None:
            convert = lambda x: x.mean if \
                      hasattr(x, 'mean') and hasattr(x, 'sdev') else x
        if len(self.data) == 0:
            return
        dat = [(convert(d) if d is not None else None) for d in self.data]
        if kwargs.get('drawstyle', None) == 'steps':
            dat = [dat[0]]+dat
            xvals = self.xbins
        else:
            xvals = self.xvals
        kwargs.pop('axes', None)
        kwargs.pop('convert', None)
        kwargs.pop('xconvert', None)
        if xconvert is not None:
            xvals = list(map(xconvert, xvals))
        return axes.plot(xvals, dat, *args, **kwargs)

    def errorbar(self, *args, **kwargs):
        """Plot errorbars with matplotlib.

        Args:
          *args: all positional arguments are passed to
            ``matplotlib.axes.Axes.errorbars``
          axes (matplotlib.axes.Axes or None, optional): Axes object to plot to.
            Uses ``matplotlib.pyplot.gca()`` if ``None``. Defaults to ``None``.
          convert (callable or None, optional): function that converts the
            elements of ``self.data`` to ``GVar`` objects. If ``None``, the
            elements of ``self.data`` must be be convertible by the ``GVar``
            constructor.
          **kwargs: all other keyword arguments are passed to
            ``matplotlib.axes.Axes.errorbars``.

        """
        axes = kwargs.get('axes', None)
        convert = kwargs.get('convert', None)
        if axes is None:
            axes = plt.gca()
        if convert is None:
            convert = lambda x: GVar(x)
        if len(self.data) == 0:
            return
        means = []
        allmeans = []
        sdevs = []
        xvals = []
        offset = kwargs.pop('offset', 0)
        for lb, x, ub, d in zip(self.xbins[:-1], self.xvals, self.xbins[1:],
                                self.data):
            if d is not None:
                gd = convert(d)
                if offset < 0:
                    xvals.append(x + (x-lb)*offset)
                else:
                    xvals.append(x + (ub-x)*offset)
                means.append(gd.mean)
                allmeans.append(gd.mean)
                sdevs.append(gd.sdev)
            else:
                allmeans.append(None)
        kwargs.pop('axes', None)
        kwargs.pop('convert', None)
        if kwargs.get('drawstyle', None) == 'steps':
            p, = axes.plot(self.xbins, [allmeans[0]]+allmeans, *args, **kwargs)
        else:
            p, = axes.plot(self.xvals, allmeans, *args, **kwargs)
        kwargs.pop('label', None)
        kwargs['ecolor'] = p.get_color()
        kwargs['elinewidth'] = p.get_linewidth()
        kwargs['fmt'] = 'none'
        kwargs['yerr'] = sdevs
        return axes.errorbar(xvals, means, *args, **kwargs)

    def setxlim(self, axes=None):
        """Set x limits of the current plot.

        Args:
          axes (matplotlib.axes.Axes or None): Axes object whose limits are
            to be set. Uses ``matplotlib.pyplot.gca()`` if ``None``. Defaults to
            ``None``.

        """
        if axes is None:
            axes = plt.gca()
        return axes.set_xlim((self.xbins[0], self.xbins[-1]))

    def __iadd__(self, h):
        if isinstance(h, Histogram1D):
            if not numpy.array_equal(h.xbins, self.xbins):
                raise ValueError('cannot add histograms with different bins')
            for p, v in numpy.ndenumerate(h.data):
                if self.data[p] is not None:
                    self.data[p] += v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                if v is not None:
                    self.data[p] += h
        return self

    def __isub__(self, h):
        if isinstance(h, Histogram1D):
            if not numpy.array_equal(h.xbins, self.xbins):
                raise ValueError('cannot add histograms with different bins')
            for p, v in numpy.ndenumerate(h.data):
                if self.data[p] is not None:
                    self.data[p] -= v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                if v is not None:
                    self.data[p] -= h
        return self

    def __imul__(self, h):
        if isinstance(h, Histogram1D):
            if not numpy.array_equal(h.xbins, self.xbins):
                raise ValueError('cannot add histograms with different bins')
            for p, v in numpy.ndenumerate(h.data):
                if self.data[p] is not None:
                    self.data[p] *= v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                if v is not None:
                    self.data[p] *= h
        return self

    def __itruediv__(self, h):
        if isinstance(h, Histogram1D):
            if not numpy.array_equal(h.xbins, self.xbins):
                raise ValueError('cannot add histograms with different bins')
            for p, v in numpy.ndenumerate(h.data):
                if self.data[p] is not None:
                    self.data[p] = self.data[p]/v
        elif h is not None:
            for p, v in numpy.ndenumerate(self.data):
                if v is not None:
                    self.data[p] = self.data[p]/h
        return self

    def __add__(self, h):
        result = self.copy()
        result += h
        return result

    def __sub__(self, h):
        result = self.copy()
        result -= h
        return result

    def __mul__(self, h):
        result = self.copy()
        result *= h
        return result

    def __truediv__(self, h):
        result = self.copy()
        result /= h
        return result

    def __radd__(self, h):
        result = self.copy()
        result += h
        return result

    def __rsub__(self, h):
        result = self.copy()
        result *= -1
        result += h
        return result

    def __rmul__(self, h):
        result = self.copy()
        result *= h
        return result

    def __rtruediv__(self, h):
        result = Histogram2D(like=self)
        for p, v in numpy.ndenumerate(self.data):
            result.data[p] = h/v
        return result


def cumulatedHistogram(h, upper=False, const=None):
    """Compute the cumulated distribution.

    Args:
      h (Histogram1D): the histogram to cumulate
      upper (bool, optional): If ``True``, the histogram is cumulated from the
        upper end. Defaults to ``False``.
      const (optional): Constant to be added to all bins of the resulting
        histogram. Defaults to ``None``, in which case nothing is added.

    Returns:
      Histogram1D: The cumulated histogram.

    Note:
      If ``hc = cumulated(h, upper=False, const=c)`` then 
      ``hc.data[i] = sum(h.data[:i+1]) + c`` for all `i` in
      ``range(len(h.data))``.
      If ``hc = cumulated(h, upper=True, const=c)`` then 
      ``hc.data[i] = sum(h.data[i:]) + c`` for all `i` in ``range(len(h.data))``.

    """
    if not isinstance(h, Histogram1D):
        raise ValueError('Argument must be Histogram1D instance.')
    result = Histogram1D(like=h)
    if len(result.data) <= 0:
        return result
    if upper:
        if const is not None:
            result.data[-1] = const + h.data[-1]
        else:
            result.data[-1] = h.data[-1]
        for i in reversed(range(len(result.data)-1)):
            result.data[i] = h.data[i] + result.data[i+1]
    else:
        if const is not None:
            result.data[0] = const + h.data[0]
        else:
            result.data[0] = h.data[0]
        for i in range(1, len(result.data)):
            result.data[i] = h.data[i] + result.data[i-1]
    return result

def densityHistogram(h):
    """Divide all bin values by the bin width (Histogram1D) or bin area
    (Histogram2D).

    Args:
      h (Histogram1D or Histogram2D): the histogram to convert. `h` is not
        modified.

    Returns:
      Histogram1D or Histogram2D: the converted histogram.

    """
    if isinstance(h, Histogram1D):
        result = Histogram1D(like=h)
        if len(result.data) <= 0:
            return result
        for i in range(len(result.xbins)-1):
            result.data[i] = h.data[i]/(h.xbins[i+1]-h.xbins[i])
        return result
    elif isinstance(h, Histogram2D):
        result = Histogram1D(like=h)
        if len(result.data) <= 0:
            return result
        for i in range(len(result.xbins)-1):
            for j in range(len(result.ybins)-1):
                result.data[j,i] = h.data[j,i]/ \
                    ((h.xbins[i+1]-h.xbins[i])*(h.ybins[j+1]-h.ybins[j]))
    else:
        raise ValueError('Argument must be Histogram1D or Histogram2D instance.')


def _split(nx, kx):
    dx = nx // kx
    if dx <= 0:
        raise ValueError(
            'Cannot split {0:i} elements into {1:i} blocks.'.format(nx, kx))
    rx = nx % kx
    ranges = []
    for sid in range(kx):
        ix = sid*(dx+1) if sid < rx else rx*(dx+1)+(sid-rx)*dx
        jx = ix + dx + 1 if sid < rx else ix + dx
        ranges.append((ix, jx))
    return ranges


class SplitHistogram1D:
    """Split 1D histogram into (roughly) equal-sized sub-histograms.

    Note:
      This class is typically used for parallelisation together with
      :py:class:`~myFitter.ParallelFunction` and
      :py:meth:`Profiler1D.scan() <myFitter.Profiler1D.scan>`::

        profiler = Profiler1D(...)
        hist = Histogram1D(...)
        splithist = SplitHistogram1D(hist, 5)
        splithist.subhists = \\
            ParallelFunction(profiler.scan, batchsize=1)(splithist.subhists)
        hist = splithist.join()

    Args:
      h (Histogram1D): The histogram to split.
      xblocks (int): The number of blocks in the x-direction.

    Attributes:
      subhists (list of Histogram1D): The list of sub-histograms.

    """
    def __init__(self, h, xblocks):
        self._xblocks = xblocks
        self._nx = len(h.xvals)
        xranges = _split(self._nx, self._xblocks)
        self.subhists = []
        for i, j in xranges:
            self.subhists.append(Histogram1D(
                xvals=h.xvals[i:j],
                xbins=h.xbins[i:j+1],
                data=h.data[i:j]))
        self._xbounds = [b.xbins[0] for b in self.subhists]
        self._xbounds.append(self.subhists[-1].xbins[-1])

    def find(self, x):
        """Find the index of the sub-histogram containing `x`.

        Args:
          x (float): x-value to search for.

        Raises:
          KeyError: None of the sub-histograms contains the x-value `x`.

        Returns:
          int: The index of the sub-histogram (in
            :py:attr:`~myFitter.SplitHistogram1D.subhists`) containing the
            x-value `x`.
            
        """
        if x < self._xbounds[0] or x > self._xbounds[-1]:
            raise KeyError('Invalid key `'+repr(x)+'`.')
        return max(bisect.bisect_left(self._xbounds, x) - 1, 0)

    def subhist(self, x):
        """Find the sub-histogram containing `x`.

        Args:
          x (float): x-value to search for.

        Raises:
          KeyError: None of the sub-histograms contains the x-value `x`.

        Returns:
          Histogram1D: The sub-histogram containing the x-value `x`.
            
        """
        return self.subhists[self.find(x)]

    def __getitem__(self, x):
        """Return the data value of a specific bin

        Args:
          x (float): An x coordinate.

        Returns:
          The data value of the bin which contains the coordinate `x`.

        Raises:
          KeyError: The coordinate `x` is outside the range of all
            sub-histograms.

        """
        return self.subhist(x)[x]

    def join(self):
        """Join all sub-histograms into a single histogram.

        Returns:
          Histogram1D: The joined histogram.

        """
        nx = self._nx
        xvals = numpy.empty(nx)
        xbins = numpy.empty(nx+1)
        data = numpy.empty(nx, dtype=object)
        i = 0
        xbins[0] = self.subhists[0].xbins[0]
        for block in self.subhists:
            for xval, xbin, v in zip(block.xvals, block.xbins[1:], block.data):
                xvals[i] = xval
                xbins[i+1] = xbin
                data[i] = v
                i = i + 1                
        return Histogram1D(xvals=xvals, xbins=xbins, data=data)


class SplitHistogram2D:
    """Split 2D histogram into (roughly) equal-sized sub-histograms.

    Note:
      This class is typically used for parallelisation together with
      :py:class:`~myFitter.ParallelFunction` and
      :py:meth:`Profiler2D.scan() <myFitter.Profiler2D.scan>`::

        profiler = Profiler2D(...)
        hist = Histogram2D(...)
        splithist = SplitHistogram2D(hist, 2, 3)
        splithist.subhists = \\
            ParallelFunction(profiler.scan, batchsize=1)(splithist.subhists)
        hist = splithist.join()

    Args:
      h (Histogram2D): The histogram to split.
      xblocks (int): The number of blocks in the x-direction.
      yblocks (int): The number of blocks in the y-direction.

    Attributes:
      subhists (list of Histogram2D): The list of sub-histograms.


    """
    def __init__(self, h, xblocks, yblocks):
        self._xblocks = xblocks
        self._yblocks = yblocks
        self._nx = len(h.xvals)
        self._ny = len(h.yvals)
        xranges = _split(self._nx, self._xblocks)
        yranges = _split(self._ny, self._yblocks)
        self.subhists = []
        for iy, jy in yranges:
            for ix, jx in xranges:
                self.subhists.append(Histogram2D(
                    xvals=h.xvals[ix:jx],
                    xbins=h.xbins[ix:jx+1],
                    yvals=h.yvals[iy:jy],
                    ybins=h.ybins[iy:jy+1],
                    data=h.data[iy:jy, ix:jx]))
        self._xbounds = [self.subhists[ib].xbins[0] \
                         for ib in xrange(self._xblocks)]
        self._xbounds.append(self.subhists[-1].xbins[-1])
        self._ybounds = [self.subhists[ib*self._xblocks].ybins[0] \
                         for ib in xrange(self._yblocks)]
        self._ybounds.append(self.subhists[-1].ybins[-1])


    def find(self, x, y):
        """Find the index of the sub-histogram containing ``(x, y)``.

        Args:
          x (float): x-coordiante of the point to find.
          y (float): y-coordiante of the point to find.

        Raises:
          KeyError: None of the sub-histograms contains the point ``(x, y)``.

        Returns:
          int: The index of the sub-histogram (in
            :py:attr:`~myFitter.SplitHistogram1D.subhists`) containing the
            point ``(x, y)``.
            
        """
        if x < self._xbounds[0] or x > self._xbounds[-1]:
            raise KeyError('Invalid key `'+repr(x)+'`.')
        ix = max(bisect.bisect_left(self._xbounds, x) - 1, 0)
        if y < self._ybounds[0] or y > self._ybounds[-1]:
            raise KeyError('Invalid key `'+repr(y)+'`.')
        iy = max(bisect.bisect_left(self._ybounds, y) - 1, 0)
        return iy*self._xblocks + ix

    def subhist(self, x, y):
        """Find the sub-histogram containing ``(x, y)``.

        Args:
          x (float): x-coordinate of the point to search for.
          y (float): y-coordinate of the point to search for.

        Raises:
          KeyError: None of the sub-histograms contains the point ``(x, y)``.

        Returns:
          Histogram2D: The sub-histogram containing the point ``(x, y)``.
            
        """
        return self.subhists[self.find(x, y)]

    def __getitem__(self, p):
        """Return the data value of a specific bin

        Args:
          p (tuple of float): A pair of x and y coordinates.

        Returns:
          The data value of the bin which contains the point `p`.

        Raises:
          KeyError: The coordinates `p` are outside the range of all
            sub-histograms.

        """
        x, y = p
        return self.subhist(x, y)[p]

    def join(self):
        """Join all sub-histograms into a single histogram.

        Returns:
          Histogram2D: The joined histogram.

        """
        nx = self._nx
        ny = self._ny
        nbx = self._xblocks
        nby = self._yblocks
        xvals = numpy.empty(nx)
        xbins = numpy.empty(nx+1)
        yvals = numpy.empty(ny)
        ybins = numpy.empty(ny+1)
        data = numpy.empty((ny, nx), dtype=object)

        xbins[0] = self.subhists[0].xbins[0]
        ybins[0] = self.subhists[0].ybins[0]
        iy = 0
        for iby in xrange(nby):
            block = self.subhists[iby*nbx]
            dy = len(block.yvals)
            yvals[iy:iy+dy] = block.yvals
            ybins[iy+1:iy+1+dy] = block.ybins[1:]
            iy = iy + dy
        ix = 0
        for ibx in xrange(nbx):
            block = self.subhists[ibx]
            dx = len(block.xvals)
            xvals[ix:ix+dx] = block.xvals
            xbins[ix+1:ix+1+dx] = block.xbins[1:]
            ix = ix + dx
        iy = 0
        for iby in xrange(nby):
            block = self.subhists[iby*nbx]
            dy = len(block.yvals)
            ix = 0
            for ibx in xrange(nbx):
                block = self.subhists[iby*nbx+ibx]
                dx = len(block.xvals)
                data[iy:iy+dy, ix:ix+dx] = block.data
                ix = ix + dx
            iy = iy + dy

        return Histogram2D(xvals=xvals, xbins=xbins,
                           yvals=yvals, ybins=ybins, data=data)


class HistoMatrix:
    def __init__(self, *args, init=0, bincenters=False):
        if len(args) % 2 != 0:
            raise ValueError(
                'HistoMatrix requires an even number of positional arguments.')
        self._varnames = [args[i] for i in range(0, len(args), 2)]
        if not all(isinstance(n, str) for n in self._varnames):
            raise ValueError('Variable names must be strings.')
        self._nameset = set(self._varnames)
        if len(self._nameset) < len(self._varnames):
            raise ValueError('Variable names must be distinct.')
        bins = [args[i] for i in range(1, len(args), 2)]
        if not all(hasattr(b, '__iter__') for b in bins):
            raise ValueError('Bin specs must be iterable.')
        
        self._histos = {}
        for xname, xbins in zip(self._varnames, bins):
            for yname, ybins in zip(self._varnames, bins):
                if xname == yname:
                    if bincenters:
                        self._histos[yname, xname] \
                            = Histogram1D(xvals=xbins, init=init)
                    else:
                        self._histos[yname, xname] \
                            = Histogram1D(xbins=xbins, init=init)
                else:
                    if bincenters:
                        self._histos[yname, xname] \
                            = Histogram2D(xvals=xbins, yvals=ybins, init=init)
                    else:
                        self._histos[yname, xname] \
                            = Histogram2D(xbins=xbins, ybins=ybins, init=init)

    def __getitem__(self, key):
        if isinstance(key, str):
            return self._histos[key, key]
        else:
            return self._histos[key]

    def inc(self, vars, by=1):
        if isinstance(vars, dict):
            for xname in self._varnames:
                for yname in self._varnames:
                    if xname == yname:
                        self._histos[yname, xname].inc(vars[xname], by=by)
                    else:
                        self._histos[yname, xname].inc(vars[xname],
                                                       vars[yname], by=by)
        elif hasattr(vars, '__len__') and len(vars) == len(self._varnames):
            for xname, xval in zip(self._varnames, vars):
                for yname, yval in zip(self._varnames, vars):
                    if xname == yname:
                        self._histos[yname, xname].inc(xval, by=by)
                    else:
                        self._histos[yname, xname].inc(xval, yval, by=by)
        else:
            raise ValueError('Invalid variable list.')

    def plot(self, diagcfg={}, offdiagcfg={}, **kwargs):
        diagcfg = diagcfg.copy()
        if 'drawstyle' not in diagcfg:
            diagcfg['drawstyle'] = 'steps'
        offdiagcfg = offdiagcfg.copy()
        if 'cmap' not in offdiagcfg:
            offdiagcfg['cmap'] = 'Blues'
        n = len(self._varnames)
        fig, axes = plt.subplots(n, n, **kwargs)
        for ix, varx in enumerate(self._varnames):
            for iy, vary in enumerate(self._varnames):
                if ix > iy:
                    axes[iy, ix].axis('off')
                    continue
                if ix == iy:
                    self._histos[vary, varx].plot(axes=axes[iy, ix],
                                                  **diagcfg)
                    self._histos[vary, varx].setxlim(axes=axes[iy, ix])
                    axes[iy, ix].set_ylim(bottom=0)
                else:
                    self._histos[vary, varx].pcolormesh(axes=axes[iy, ix],
                                                        **offdiagcfg)
                    self._histos[vary, varx].setxlim(axes=axes[iy, ix])
                    self._histos[vary, varx].setylim(axes=axes[iy, ix])
        for i, varname in enumerate(self._varnames):
            axes[n-1, i].set_xlabel(varname)
            if i > 0:
                axes[i, 0].set_ylabel(varname)
            
