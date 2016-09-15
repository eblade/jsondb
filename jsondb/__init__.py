#!/usr/bin/env python3

import os
import json
import threading
import shutil
import logging
import blist
import hashlib


class Database:
    def __init__(self, root=None):
        if root is None:
            raise ValueError('root cannot be None')
        logging.debug('Initializing JsonDB at %s', root)
        self.root = root
        self._view_function = dict()
        self._view_data = dict()
        self._object_folder = os.path.join(self.root, 'objects')
        self._id_counter_file = os.path.join(self.root, 'id_counter')
        self._lock = threading.Lock()
        self._setup()

    def destroy(self):
        logging.debug('Destroying JsonDB at %s', self.root)
        with self._lock:
            if self.root:
                shutil.rmtree(self.root)

    def _setup(self):
        os.makedirs(self._object_folder, exist_ok=True)

    def _next_id(self):
        try:
            with open(self._id_counter_file, 'r') as f:
                current = int(f.readline())
        except IOError:
            current = 0
        with open(self._id_counter_file, 'w') as f:
            f.write(str(current + 1))
        return current

    def set_next_id(self, id):
        id = int(id)
        with self._lock:
            with open(self._id_counter_file, 'w') as f:
                f.write(str(id))

    def _get_object_filename(self, id):
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

            if '_rev' not in o:
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
                challenge_rev = int(o['_rev'])
                if current_rev != challenge_rev:
                    raise Conflict
                o['_rev'] = current_rev + 1

            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'wb') as f:
                s = json.dumps(o, indent=2)
                f.write(s.encode('utf8'))

            self._review(o, delete=True, add=True)
            return o

    def define(self, view_name, fn):
        with self._lock:
            self._view_function[view_name] = fn
            self._view_data[view_name] = \
                blist.sortedlist(key=view_key)
        self.reindex(views=[view_name])

    def reindex(self, views=all):
        with self._lock:
            logging.info("Generating views (%s)", "all" if views is all else ', '.join(views))
            count = 0
            for name in sorted(self._view_function.keys()):
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
            logging.info("Read %i objects.", count)

    def view(self, view_name, key=any, startkey=None, endkey=any,
             include_docs=False):
        if key is not any and None not in (startkey, endkey):
            raise ValueError('Either key or startkey/endkey valid')

        with self._lock:
            view_data = self._view_data[view_name]

            if key is not any:
                key = {'key': key}
                startindex = view_data.bisect_left(key)
                key_ref = view_key(key)
                endindex = None

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
                    logging.info('l=%d, r=%d -> startindex=%d', l, r, startindex)

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
                    logging.info('l=%d, r=%d -> endindex=%d', l, r, endindex)

            for v in view_data[startindex:endindex]:
                if key is not any:
                    if key_ref != view_key(v):
                        break
                if include_docs:
                    v = dict(v)
                    v['doc'] = self._get(v['id'])
                yield v

    def _review(self, o, delete=False, add=False, views=all):
        id = o['_id']

        def create_view_data(o, row):
            k, v = row
            return {
                'id': o['_id'],
                'key': k,
                'value': v,
            }

        for name, fn in self._view_function.items():
            if views is not all and name not in views:
                continue
            try:
                view_data = self._view_data[name]
            except KeyError:
                view_data = blist.sortedlist(key=view_key)
                self._view_data[name] = view_data

            if delete:
                to_delete = []
                for index, v in enumerate(view_data):
                    if v['id'] == id:
                        to_delete.append(index)
                for index in reversed(to_delete):
                    del view_data[index]

            if add:
                rows = fn(o)
                if rows is None:
                    continue
                if hasattr(rows, '__next__'):
                    for row in rows:
                        view_data.add(create_view_data(o, row))
                else:
                    view_data.add(create_view_data(o, rows))


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
