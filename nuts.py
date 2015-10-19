__all__ = ['NutsRegions']

import fiona
import shapely.geometry as geo

class NutsRegions:
    def __init__(self, filename=None,
                 id_key='NUTS_ID', level_key='STAT_LEVL_', country='UK'):
        self._root = {'shape' : None}
        if filename is not None:
            self.read(filename, id_key=id_key, level_key=level_key,
                      country=country)

    def add(self, id, shape):
        if type(id) is not str or len(id) < 2:
            raise ValueError('Invalid NUTS ID.')
        
        ids = [id[:2]] + [c for c in id[2:]]
        region = self._root
        for c in ids:
            if c not in region:
                region[c] = {'shape' : None}
            region = region[c]

        region['shape'] = shape

    def read(self, fname, id_key='NUTS_ID', level_key='STAT_LEVL_',
             country='UK'):
        with fiona.open(fname) as shapefile:
            for shape in shapefile:
                id = shape['properties'][id_key]
                level = shape['properties'][level_key]
                if len(id) < 2 or id[:2] != country:
                    continue
                self.add(id, geo.shape(shape['geometry']))

    def _getdict(self, id):
        ids = [id[:2]] + [c for c in id[2:]]
        region = self._root
        for c in ids:
            if c not in region:
                raise KeyError(id)
            region = region[c]
        return region
        
    def __getitem__(self, id):
        return self._getdict(id)['shape']

    def _yieldregions(self, level, prefix, d):
        if level == 0:
            yield prefix, d['shape']
        else:
            for id, subregion in d.items():
                if id == 'shape':
                    continue
                for item in self._yieldregions(level-1, prefix+id,
                                               subregion):
                    yield item

    def level(self, level, id=None):
        regiondict = self._root
        idlevel = -1
        if id is not None:
            if type(id) is not str or len(id) < 2:
                raise ValueError('Invalid NUTS ID.')
            if len(id)-2 > level:
                id = id[:level+2]
            regiondict = self._getdict(id)
            idlevel = len(id)-2
        else:
            id = ''
        for item in self._yieldregions(level-idlevel, id, regiondict):
            yield item

    def find(self, point, level=3):
        currentlevel = 0
        currentid = None
        while currentlevel <= level:
            newid = None
            for id, shape in self.level(currentlevel, id=currentid):
                if shape.contains(point):
                    newid = id
                    break
            if not newid:
                return currentid
            currentid = newid
            currentlevel += 1
        return currentid


if __name__ == '__main__':
    import matplotlib.pyplot as plt
    from mpl_toolkits.basemap import Basemap
    from plotshape import plotShape

    tolerance = 1e-2
    minarea = tolerance**2

    nuts = NutsRegions('NUTS_2013_SHP/data/NUTS_RG_01M_2013.shp')
    m = Basemap(projection='ortho',
                lat_0=52.0,
                lon_0=0.0,
                llcrnrx=-600e3,
                llcrnry=-300e3,
                urcrnrx=+200e3,
                urcrnry=+900e3,
                resolution='l')
    m.drawmapboundary(fill_color='white')

    for id, shape in nuts.level(3, 'UK'):
        plotShape(shape, m, minarea=minarea, edgecolor='k', facecolor='none')
    for id, shape in nuts.level(2, 'UK'):
        plotShape(shape, m, minarea=minarea, edgecolor='g', facecolor='none')
    for id, shape in nuts.level(1, 'UK'):
        plotShape(shape, m, minarea=minarea, edgecolor='b', facecolor='none')

    plt.show()
    
