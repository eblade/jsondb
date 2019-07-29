#!/usr/bin/env python3

import os
import json
import threading
import shutil
import logging
import blist
import hashlib


__version__ = '0.2.0'
__author__ = 'Johan Egneblad <johan@egneblad.com>'
__all__ = ['Database', 'Conflict']


class Database:
    def __init__(self, root=None, id_generator=None, filename_hasher=None, logger=None):
        if root is None:
            raise ValueError('root cannot be None')
        self.logger = logger or logging.getLogger('jsondb')
        self.logger.debug('Initializing JsonDB at %s', root)
        self.root = root
        self._id_generator = id_generator or self._default_id_generator
        self._filename_hasher = filename_hasher or self._default_filename_hasher
        self._view_map_function = dict()
        self._view_reduce_function = dict()
        self._view_data = dict()
        self._id_view_cache = dict()
        self._object_folder = os.path.join(self.root, 'objects')
        self._id_counter_file = os.path.join(self.root, 'id_counter')
        self._lock = threading.Lock()
        self._setup()

    def destroy(self):
        self.logger.debug('Destroying JsonDB at %s', self.root)
        with self._lock:
            if self.root:
                shutil.rmtree(self.root)

    def clear(self):
        self.logger.debug('Clear JsonDB at %s', self.root)
        with self._lock:
            shutil.rmtree(self._object_folder)
            self._view_data = {view: blist.sortedlist()
                               for view in self._view_data.keys()}
            self._id_view_cache = dict()

    def _setup(self):
        os.makedirs(self._object_folder, exist_ok=True)

    def _next_id(self):
        return self._id_generator()

    def _default_id_generator(self):
        try:
            with open(self._id_counter_file, 'r') as f:
                current = int(f.readline())
        except IOError:
            current = 0
        with open(self._id_counter_file, 'w') as f:
            f.write(str(current + 1))
        return current

    def _get_object_filename(self, id):
        return self._filename_hasher(id)

    def _default_filename_hasher(self, id):
        hash_name = hashlib.sha224(str(id).encode('utf8')).hexdigest()
        return os.path.join(hash_name[:2], hash_name[2:] + '.json')

    def __setitem__(self, id, o):
        o['_id'] = id
        self.save(o)

    def has(self, id):
        with self._lock:
            path = os.path.join(
                self._object_folder,
                self._get_object_filename(id)
            )
            return os.path.exists(path)

    def get(self, id):
        with self._lock:
            return self._get(id)

    def _get(self, id):
        path = os.path.join(
            self._object_folder,
            self._get_object_filename(id)
        )
        try:
            with open(path, 'rb') as f:
                s = f.read().decode('utf8')
                o = json.loads(s)
                return o
        except FileNotFoundError:
            raise KeyError('Key does not exist: ' + str(id))

    def __getitem__(self, key):
        return self.get(key)

    def delete(self, id):
        with self._lock:
            path = os.path.join(
                self._object_folder,
                self._get_object_filename(id)
            )
            os.remove(path)
            self._review({'_id': id}, delete=True)

    def save(self, o):
        with self._lock:
            id = o.get('_id')
            if id is None:
                id = self._next_id()
                o['_id'] = id
                o['_rev'] = 0

            path = os.path.join(
                self._object_folder,
                self._get_object_filename(id)
            )

            try:
                with open(path, 'rb') as f:
                    s = f.read().decode('utf8')
                    o_current = json.loads(s)
            except IOError:
                o_current = None

            if o_current is not None:
                current_rev = int(o_current['_rev'])
                if '_rev' not in o:
                    raise Conflict
                if o['_rev'] is None:
                    raise Conflict
                challenge_rev = int(o['_rev'])
                if current_rev != challenge_rev:
                    raise Conflict
                o['_rev'] = current_rev + 1
            else:
                if '_rev' not in o:
                    o['_rev'] = 0
                elif o['_rev'] is None:
                    o['_rev'] = 0

            os.makedirs(os.path.dirname(path), exist_ok=True)
            s = json.dumps(o, indent=2)
            with open(path, 'wb') as f:
                f.write(s.encode('utf8'))

            self._review(o, delete=True, add=True)
            return o

    def define(self, view_name, map_fn, reduce_fn=None):
        with self._lock:
            self._view_map_function[view_name] = map_fn
            self._view_reduce_function[view_name] = reduce_fn
            self._view_data[view_name] = \
                blist.sortedlist(key=view_key)
        self.reindex(views=[view_name])

    def reindex(self, views=all):
        with self._lock:
            self.logger.info("Generating views (%s)", "all" if views is all else ', '.join(views))
            count = 0
            for name in sorted(self._view_map_function.keys()):
                if views is all or name in views:
                    self._view_data[name] = \
                        blist.sortedlist(key=view_key)
            for r, ds, fs in os.walk(self._object_folder):
                for f in fs:
                    if f.endswith('.json'):
                        path = os.path.join(r, f)
                        with open(path, 'rb') as f:
                            s = f.read().decode('utf8')
                            o = json.loads(s)
                            self._review(o, add=True, views=views)
                            count += 1
            self.logger.info("Indexed %i object%s.", count, 's' if count != 1 else '')

    def view(self, view_name, key=any, startkey=None, endkey=any,
             include_docs=False, group=False, no_reduce=False,
             skip=0, limit=None):

        with self._lock:
            view_data = self._view_data[view_name]

            if key is not any:
                key = {'key': key}
                startindex = view_data.bisect_left(key)
                key_ref = view_key(key)
                endindex = len(view_data)

            else:
                if startkey is None:
                    startindex = 0
                elif startkey is any:
                    startindex = len(view_data)
                else:
                    startkey = {'key': startkey}
                    l = view_data.bisect_left(startkey)
                    r = view_data.bisect_right(startkey)
                    if l == r:
                        startindex = l
                    else:
                        startindex = l

                if endkey is None:
                    endindex = 0
                elif endkey is any:
                    endindex = len(view_data)
                else:
                    endkey = {'key': endkey}
                    l = view_data.bisect_left(endkey)
                    r = view_data.bisect_right(endkey)
                    if l == r:
                        endindex = l
                    else:
                        endindex = r

            reduce_fn = self._view_reduce_function[view_name]
            if reduce_fn is None or no_reduce:
                counter = 0
                for v in view_data[startindex:endindex]:
                    counter += 1
                    if key is not any:
                        if key_ref != view_key(v):
                            break
                    if counter <= skip:
                        continue
                    if include_docs:
                        v = dict(v)
                        v['doc'] = self._get(v['id'])
                    yield v
            else:
                if group:
                    last_key = None
                    values = []
                    for v in view_data[startindex:endindex]:
                        if key is not any:
                            if key_ref != view_key(v):
                                break
                        this_key = v['key']
                        if last_key is not None and this_key != last_key:
                            yield {'key': last_key, 'value': reduce_fn([last_key], values, False)}
                            values = []
                        last_key = this_key
                        values.append(v['value'])
                    if len(values) > 0:
                        yield {'key': this_key, 'value': reduce_fn([this_key], values, False)}
                else:
                    raise NotImplementedError('Reduce without grouping not implemented')

    def _review(self, o, delete=False, add=False, views=all):
        id = o['_id']

        def create_view_data(o, row):
            k, v = row
            return {
                'id': o['_id'],
                'key': k,
                'value': v,
            }

        for name, fn in self._view_map_function.items():
            if views is not all and name not in views:
                continue
            try:
                view_data = self._view_data[name]
                id_view_cache = self._id_view_cache[name]
            except KeyError:
                view_data = blist.sortedlist(key=view_key)
                self._view_data[name] = view_data
                id_view_cache = dict()
                self._id_view_cache[name] = id_view_cache

            if delete and id in id_view_cache.keys():
                for v in id_view_cache[id]:
                    try:
                        view_data.remove(v)
                    except ValueError:
                        pass
                del id_view_cache[id]

            if add:
                try:
                    this_id_view_cache = id_view_cache[id]
                except KeyError:
                    this_id_view_cache = list()
                    id_view_cache[id] = this_id_view_cache

                rows = fn(o)
                if rows is None:
                    continue
                if hasattr(rows, '__next__'):
                    for row in rows:
                        v = create_view_data(o, row)
                        view_data.add(v)
                        this_id_view_cache.append(v)
                else:
                    v = create_view_data(o, rows)
                    view_data.add(v)
                    this_id_view_cache.append(v)


class Conflict(Exception):
    pass


def view_key(value):
    return Key(value['key']), Optional(value.get('id'))


def Key(key):
    if isinstance(key, tuple):
        return tuple(Comparable(x) for x in key)
    else:
        return key,


class Comparable:
    def __init__(self, v):
        self.v = v

    def __repr__(self):
        return '<%s>' % repr(self.v)

    def __lt__(self, other):
        if self.v is any and other.v is any:
            return False
        elif self.v is any:
            return False
        elif other.v is any:
            return True
        elif self.v is None and other.v is None:
            return False
        elif self.v is None:
            return True
        elif other.v is None:
            return False
        try:
            return self.v < other.v
        except TypeError:
            return str(self.v) < str(other.v)

    def __le__(self, other):
        if self.v is any and other.v is any:
            return True
        elif self.v is any:
            return False
        elif other.v is any:
            return True
        elif self.v is None and other.v is None:
            return True
        elif self.v is None:
            return True
        elif other.v is None:
            return False
        try:
            return self.v <= other.v
        except TypeError:
            return str(self.v) <= str(other.v)

    def __eq__(self, other):
        return self.v == other.v

    def __ne__(self, other):
        return not (self == other)


class Optional(Comparable):
    def __eq__(self, other):
        if self.v is None or other.v is None:
            return True
        else:
            return super().__eq__(other)
