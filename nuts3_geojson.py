import conf
from nuts import NutsRegions
import shapely.geometry as geo
import json
import csv


nuts = NutsRegions(conf.NUTS_DATA)

nutsnames = {}
with open('nutsregions/nuts3.csv', 'r', newline='') as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        if len(row) != 2:
            continue
        nutsnames[row[0]] = row[1]


features = []
for id, (nutsid, shape) in enumerate(nuts.level(3)):
    geometry = geo.mapping(shape)
    if nutsid not in nutsnames:
        print('Description missing for NUTS ID: '+nutsid)
    features.append({'type' : 'Feature',
                     'id' : id,
                     'properties' : {
                         'nutsId' : nutsid,
                         'count'  : 0,
                         'name'   : nutsnames.get(nutsid, '')
                     },
                     'geometry' : geometry})

geojson = {'type' : 'FeatureCollection',
           'features' : features}

with open('nuts3regions.json', 'w') as jsonfile:
    json.dump(geojson, jsonfile)


