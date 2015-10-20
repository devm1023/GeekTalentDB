import conf
from nuts import NutsRegions
import shapely.geometry as geo
import json


nuts = NutsRegions(conf.NUTS_DATA)

features = []
for id, (nutsid, shape) in enumerate(nuts.level(3)):
    geometry = geo.mapping(shape)
    features.append({'type' : 'Feature',
                     'id' : id,
                     'properties' : {
                         'nutsId' : nutsid,
                         'count'  : 0
                     },
                     'geometry' : geometry})

geojson = {'type' : 'FeatureCollection',
           'features' : features}

with open('nuts3regions.json', 'w') as jsonfile:
    json.dump(geojson, jsonfile)


