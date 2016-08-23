__all__ = ['EntityMapper']

from canonicaldb import Entity
from textnormalization import normalized_entity, normalized_sector
import csv

class EntityMapper:
    def __init__(self, db, filename):
        self._db = db
        self._nrm_maps = {None : {}}
        self._inv_nrm_maps = {None : {}}
        self._names = {}
        if not filename:
            return
        with open(filename, 'r') as csvfile:
            csvreader = csv.reader(row for row in csvfile \
                                   if not row.strip().startswith('#'))
            rowcount = 0
            for row in csvreader:
                rowcount += 1
                if len(row) != 5:
                    raise IOError('Invalid row in CSV file:\n{0:s}' \
                                  .format(repr(row)))
                type = row[0].strip()
                language = row[1].strip()
                sector = row[2].strip()
                if not sector or type == 'sector':
                    sector = None
                entity1 = row[3].strip()
                entity2 = row[4].strip()
                nrm_sector = normalized_entity('sector', 'linkedin', language,
                                               sector)
                if nrm_sector not in self._nrm_maps:
                    self._nrm_maps[nrm_sector] = {}
                    self._inv_nrm_maps[nrm_sector] = {}
                nrm_map = self._nrm_maps[nrm_sector]
                inv_nrm_map = self._inv_nrm_maps[nrm_sector]
                nrm_entity1 = normalized_entity(type, 'linkedin', language,
                                                entity1)
                nrm_entity2 = normalized_entity(type, 'linkedin', language,
                                                entity2)
                if entity1 == 'M D':
                    raise SystemExit()
                if not nrm_entity1:
                    raise IOError('Invalid row in CSV file:\n{0:s}\n' \
                                  .format(repr(row)))
                if nrm_entity1 in nrm_map:
                    raise IOError('Duplicate entry in CSV file:\n{0:s}\n' \
                                  .format(repr(row)))
                if nrm_entity1 != nrm_entity2 and \
                   (nrm_entity1 in inv_nrm_map \
                    or nrm_entity1 in self._inv_nrm_maps[None]):
                    raise IOError('Circular mapping in CSV file:\n{0:s}\n' \
                                  .format(repr(row)))
                nrm_map[nrm_entity1] = nrm_entity2
                if nrm_entity2 not in inv_nrm_map:
                    inv_nrm_map[nrm_entity2] = set()
                inv_nrm_map[nrm_entity2].add(nrm_entity1)
                self._names[nrm_entity1] = entity1
                self._names[nrm_entity2] = entity2

    def __call__(self, entity, sector=None, nrm_sector=None, language='en'):
        if sector is not None:
            nrm_sector = normalized_entity('sector', 'linkedin', language,
                                           sector)
        if nrm_sector in self._nrm_maps[None]:
            nrm_sector = self._nrm_maps[None][nrm_sector]
        if nrm_sector is not None and nrm_sector in self._nrm_maps:
            nrm_map = self._nrm_maps[nrm_sector]
            if entity in nrm_map:
                return nrm_map[entity]
        nrm_map = self._nrm_maps[None]
        if entity in nrm_map:
            return nrm_map[entity]
        return entity

    def entities(sectors=None):
        items = set()
        if sectors is None:
            for sector, inv_map in self._inv_nrm_maps.items():
                items.update(inv_map.keys())
        else:
            for sector in sectors:
                items.update(self._inv_nrm_maps.get(sector, {}).keys())
        items = list(items)
        items.sort()
        for item in items:
            yield item

    def inv(self, entity, sector=None, nrm_sector=None, language='en',
            sector_specific=False):
        if sector is not None:
            nrm_sector = normalized_entity('sector', 'linkedin', language,
                                           sector)
        result = set([entity])
        if nrm_sector in self._nrm_maps[None]:
            nrm_sector = self._nrm_maps[None][nrm_sector]
        if not sector_specific:
            inv_nrm_map = self._inv_nrm_maps[None]
            result.update(inv_nrm_map.get(entity, []))
        if nrm_sector is not None:
            inv_nrm_map = self._inv_nrm_maps.get(nrm_sector, {})
            result.update(inv_nrm_map.get(entity, []))
        return result

    def name(self, entity):
        if entity in self._names:
            return self._names[entity]
        else:
            name = self._db.query(Entity.name) \
                           .filter(Entity.nrm_name == entity) \
                           .first()
            if name is None:
                raise LookupError('Could not find name for entity {0:s}.' \
                                  .format(repr(entity)))
            return name[0]
