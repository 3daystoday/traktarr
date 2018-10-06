import json
import logging

from sqlitedict import SqliteDict

log = logging.getLogger(__name__)


class Cache:
    def __init__(self, cache_file: str, cfg: dict):
        self.cache_file = cache_file
        self.cfg = cfg
        # load caches
        self.caches = {
            # movies
            'movies_popular': SqliteDict(self.cache_file, tablename='movies_popular', encode=json.dumps,
                                         decode=json.loads, autocommit=False),
            'movies_trending': SqliteDict(self.cache_file, tablename='movies_trending', encode=json.dumps,
                                          decode=json.loads, autocommit=False),
            # shows
            'shows_popular': SqliteDict(self.cache_file, tablename='shows_popular', encode=json.dumps,
                                        decode=json.loads, autocommit=False),
            'shows_trending': SqliteDict(self.cache_file, tablename='shows_trending', encode=json.dumps,
                                         decode=json.loads, autocommit=False)
        }

    def get_cached_items(self, media_type: str, list_type: str):
        cache_name = '%s_%s' % (media_type, list_type)
        cache = self.caches[cache_name] if cache_name in self.caches else None
        if cache is None:
            log.error("Failed to retrieve cache for %s", cache_name)
            return []
        items = []
        for k, v in cache.items():
            items.append(v)
        return items

    def add_cached_items(self, media_type: str, list_type: str, trakt_data: list):
        made_changes = False
        cache_name = '%s_%s' % (media_type, list_type)
        cache = self.caches[cache_name] if cache_name in self.caches else None
        if cache is None:
            log.error("Failed to retrieve cache for %s", cache_name)
            return False

        # loop each list item, adding to the cache dict
        id_dict = media_type.rstrip('s')
        id_key = 'tvdb' if media_type == 'shows' else 'tmdb'
        for item in trakt_data:
            item_key = None
            try:
                item_key = item[id_dict]['ids'][id_key]
            except Exception:
                log.error("Failed finding %s id for: %s", id_key, json.dumps(item))

            # did we find an id to uniquely identify this item?
            if not item_key:
                continue
            # did an item already exist in the cache with this id?
            if item_key in cache:
                log.debug("A %s item already existed with %s id: %s", cache_name, id_key, item_key)
                continue

            # add item to cache
            cache[item_key] = item
            made_changes = True

        if made_changes:
            self.save_cache(media_type, list_type)
        return True

    def remove_cached_item(self, media_type: str, list_type: str, trakt_item: dict):
        cache_name = '%s_%s' % (media_type, list_type)
        cache = self.caches[cache_name] if cache_name in self.caches else None
        if cache is None:
            log.error("Failed to retrieve cache for %s", cache_name)
            return False

        id_dict = media_type.rstrip('s')
        id_key = 'tvdb' if media_type == 'shows' else 'tmdb'
        item_key = None
        try:
            item_key = trakt_item[id_dict]['ids'][id_key]
        except Exception:
            log.error("Failed finding %s id for: %s", id_key, json.dumps(trakt_item))
            return False

        removed = cache.pop(item_key, None)
        return True if removed is not None else False

    def save_cache(self, media_type: str = None, list_type: str = None):
        if not media_type or not list_type:
            for cache_name, cache in self.caches:
                cache.commit()
            return True

        # commit specific cache
        cache_name = '%s_%s' % (media_type, list_type)
        cache = self.caches[cache_name] if cache_name in self.caches else None
        if not cache:
            log.error("Failed to retrieve cache for %s", cache_name)
            return False
        cache.commit()
        return True
