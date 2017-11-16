'''
Converts NUTS data to GeoJSON for insights
'''
import json
import csv

import shapely.geometry as geo

import conf
from nuts import NutsRegions

countries = [
    'AT', 'BE', 'BG', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'EL', 'ES', 'FI', 'FR', 'HR',
    'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'ME', 'MK', 'MT', 'NL', 'NO', 'PL',
    'PT', 'RO', 'SE', 'SI', 'SK', 'TR', 'UK'
]

nutsnames = {}

for level in range(1, 4):
    with open('nutsregions/nuts{}.csv'.format(level), 'r', newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            if len(row) != 2:
                continue
            nutsnames[row[0]] = row[1]

for country in countries:
    print(country)
    nuts = NutsRegions(conf.NUTS_DATA, countries=[country])

    country = country.lower()

    if country == 'uk':
        country = 'gb'
    if country == 'el':
        country = 'gr'

    for level in range(1, 4):
        features = []
        for id, (nutsid, shape) in enumerate(nuts.level(level)):
            geometry = geo.mapping(shape)

            if nutsid not in nutsnames:
                print('Description missing for NUTS ID: '+nutsid)
            features.append({'type' : 'Feature',
                            'id' : id,
                            'properties' : {
                                'nutsId' : nutsid,
                                #'count'  : 0,
                                'name'   : nutsnames.get(nutsid, '')
                            },
                            'geometry' : geometry})

        geojson = {'type' : 'FeatureCollection',
                'features' : features}

        with open('nutsregions/{}_nuts{}.json'.format(country.lower(), level), 'w') as jsonfile:
            json.dump(geojson, jsonfile)


nuts = NutsRegions(conf.NUTS_DATA, countries=countries)

bounds = {}
for id, (nutsid, shape) in enumerate(nuts.level(0)):
    geometry = geo.mapping(shape)

    country = nutsid.lower()

    if country == 'uk':
        country = 'gb'
    if country == 'el':
        country = 'gr'
    bounds[country] = [[shape.bounds[1], shape.bounds[0]], [shape.bounds[3], shape.bounds[2]]]

with open('nutsregions/country_bounds.json', 'w') as jsonfile:
    json.dump(bounds, jsonfile, indent=4)
