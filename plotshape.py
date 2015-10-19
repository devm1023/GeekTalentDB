__all__ = ['plotShape']

import matplotlib.pyplot as plt
from matplotlib.path import Path
from matplotlib.patches import PathPatch, Polygon
import numpy as np


def ring_coding(ob):
    # The codes will be all "LINETO" commands, except for "MOVETO"s at the
    # beginning of each subpath
    n = len(ob.coords)
    codes = np.ones(n, dtype=Path.code_type) * Path.LINETO
    codes[0] = Path.MOVETO
    return codes

def mappedarray(r, map):
    if map is not None:
        return np.array(map(*r.coords.xy)).T
    else:
        return np.asarray(r)
    

def pathify(polygon, map, minarea):
    # Convert coordinates to path vertices. Objects produced by Shapely's
    # analytic methods have the proper coordinate order, no need to sort.
    interiors = [r for r in polygon.interiors if r.area >= minarea]
    vertices = np.concatenate(
                    [mappedarray(polygon.exterior, map)]
                    + [mappedarray(r, map) for r in interiors])
    codes = np.concatenate(
                [ring_coding(polygon.exterior)]
                + [ring_coding(r) for r in interiors])
    return Path(vertices, codes)


def plotShape(shape, map=None, minarea=None, axes=None, **kwargs):
    if axes is None:
        axes = plt.gca()

    if shape.geom_type == 'Polygon':
        if minarea is None or shape.area >= minarea:
            axes.add_patch(PathPatch(pathify(shape, map, minarea), **kwargs))
    elif shape.geom_type == 'MultiPolygon':
        for poly in shape:
            plotShape(poly, map=map, minarea=minarea, axes=axes, **kwargs)
    else:
        raise ValueError('Geometry type must be \'Polygon\' or \'MultiPolygon\'')

