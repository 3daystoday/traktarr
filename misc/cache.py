import json
import logging
from copy import copy
from datetime import datetime, timedelta

from sqlitedict import SqliteDict

log = logging.getLogger(__name__)


class Cache:
    def __init__(self, cache_file: str, cfg: dict):
        self.cache_file = cache_file
        self.cfg = cfg
        # load caches
        self.caches = {}

    def get_correct_cache(self, media_type: str, list_type: str):
        cache_name = '%s_%s' % (media_type, list_type)
        cache = self.caches[cache_name] if cache_name in self.caches else None
        if cache is None:
            cache = SqliteDict(self.cache_file, tablename=cache_name, encode=json.dumps,
                               decode=json.loads, autocommit=False)
            self.caches[cache_name] = cache
        return cache

    def get_cached_items(self, media_type: str, list_type: str):
        cache_name = "%s_%s" % (media_type, list_type)
        cache = self.get_correct_cache(media_type, list_type)
        if cache is None:
            log.error("Failed to retrieve cache for %s", cache_name)
            return []

        items = []
        for k, v in cache.items():
            items.append(v)
        return items

    def add_cached_items(self, media_type: str, list_type: str, trakt_data: list):
        made_changes = False
        added_items = 0
        items_expire = self.get_time_in_future(self.cfg['cache']['expires_in_days'])

        cache_name = "%s_%s" % (media_type, list_type)
        cache = self.get_correct_cache(media_type, list_type)
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
            prepared_item = {'cache_expires': items_expire}
            prepared_item.update(item)
            cache[item_key] = prepared_item
            made_changes = True
            added_items += 1

        if made_changes:
            self.save_cache(media_type, list_type)
            log.info("Added %d items to the %s cache", added_items, cache_name)
        return True

    def remove_cached_item(self, media_type: str, list_type: str, trakt_item: dict):
        cache_name = "%s_%s" % (media_type, list_type)
        cache = self.get_correct_cache(media_type, list_type)
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

        removed = cache.pop(item_key)
        return True if removed is not None else False

    def save_cache(self, media_type: str = None, list_type: str = None):
        if not media_type or not list_type:
            for cache_name, cache in self.caches:
                cache.commit()
            return True

        # commit specific cache
        cache_name = "%s_%s" % (media_type, list_type)
        cache = self.get_correct_cache(media_type, list_type)
        if cache is None:
            log.error("Failed to retrieve cache for %s", cache_name)
            return False
        cache.commit()
        return True

    def prune_expired_cache_items(self, media_type: str, list_type: str, cache_items: list):
        pruned_items = 0
        cache_name = "%s_%s" % (media_type, list_type)
        cache = self.get_correct_cache(media_type, list_type)
        if cache is None:
            log.error("Failed to retrieve cache for %s", cache_name)
            return 0

        for item in copy(cache_items):
            # remove cache items that have no expiration (should never occur)
            if 'cache_expires' not in item:
                log.debug("No cache_expires field for cache item: %s", item)
                self.remove_cached_item(media_type, list_type, item)
                pruned_items += 1
                cache_items.remove(item)
                continue

            # remove expired items
            if datetime.now() > datetime.strptime(item['cache_expires'], "%Y-%m-%d %H:%M:%S.%f"):
                # item has expired
                log.debug("Cache item has expired: %s", item)
                self.remove_cached_item(media_type, list_type, item)
                pruned_items += 1
                cache_items.remove(item)
                continue

        if pruned_items:
            # save changes
            self.save_cache(media_type, list_type)
        return pruned_items

    @staticmethod
    def get_time_in_future(days):
        now = datetime.now()
        future = timedelta(days=days)
        return str(now + future)
