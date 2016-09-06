#!/usr/bin/env python3

import os
import json
import threading
import shutil
import logging
import sortedcontainers


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
        self.lock = threading.Lock()
        self._setup()

    def destroy(self):
        logging.debug('Destroying JsonDB at %s', self.root)
        with self.lock:
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

    def _get_object_filename(self, id):
        hex_name = '%016x' % (id)
        return os.path.join(hex_name[-2:], hex_name + '.json')

    def add(self, o):
        with self.lock:
            id = self._next_id()
            path = os.path.join(
                self._object_folder,
                self._get_object_filename(id)
            )
            os.makedirs(os.path.dirname(path), exist_ok=True)
            o['_id'] = id
            o['_rev'] = 0
            with open(path, 'wb') as f:
                s = json.dumps(o, indent=2, sort_keys=True)
                f.write(s.encode('utf8'))
            self._review(o, delete=True, add=True)
            return o

    def has(self, id):
        with self.lock:
            path = os.path.join(
                self._object_folder,
                self._get_object_filename(id)
            )
            return os.path.exists(path)

    def get(self, id):
        with self.lock:
            path = os.path.join(
                self._object_folder,
                self._get_object_filename(id)
            )
            with open(path, 'rb') as f:
                s = f.read().decode('utf8')
                o = json.loads(s)
                return o

    def delete(self, id):
        with self.lock:
            path = os.path.join(
                self._object_folder,
                self._get_object_filename(id)
            )
            os.remove(path)
            self._review({'_id': id}, delete=True)

    def update(self, o):
        with self.lock:
            path = os.path.join(
                self._object_folder,
                self._get_object_filename(o['_id'])
            )
            with open(path, 'rb') as f:
                s = f.read().decode('utf8')
                o_current = json.loads(s)
            if o['_rev'] != o_current['_rev']:
                raise Conflict
            o['_rev'] += 1
            with open(path, 'wb') as f:
                s = json.dumps(o, indent=2, sort_keys=True)
                f.write(s.encode('utf8'))
            self._review(o, delete=True, add=True)
            return o

    def define(self, view_name, fn):
        with self.lock:
            self._view_function[view_name] = fn
            self._view_data[view_name] = \
                sortedcontainers.SortedDict()
        self.reindex(views=[view_name])

    def reindex(self, views=all):
        with self.lock:
            logging.info("Generating views...")
            count = 0
            for name in self._view_function.keys():
                if views is all or name in views:
                    self._view_data[name] = \
                        sortedcontainers.SortedDict()
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

    def view(self, view_name, key=any, startkey=None, endkey=any, expand=False):
        if key is not any and None not in (startkey, endkey):
            raise ValueError('Either key or startkey/endkey valid')

        if isinstance(startkey, tuple):
            startkey = tuple(Comparable(x) for x in startkey)
        else:
            startkey = Comparable(startkey),

        if isinstance(endkey, tuple):
            endkey = tuple(Comparable(x) for x in endkey)
        else:
            endkey = Comparable(endkey),

        with self.lock:
            for k, v in self._view_data[view_name].items():
                if v is None:
                    continue

                if key is not any:
                    if key == k:
                        for d in v:
                            yield d

                else:
                    if isinstance(k, tuple):
                        k = tuple(Comparable(x) for x in k)
                    else:
                        k = Comparable(k),

                    if k < startkey:
                        continue
                    elif k > endkey:
                        raise StopIteration

                    for d in v:
                        yield d

    def _review(self, o, delete=False, add=False, views=all):
        id = o['_id']
        for name, fn in self._view_function.items():
            if views is not all and name not in views:
                continue
            try:
                view_data = self._view_data[name]
            except KeyError:
                view_data = sortedcontainers.SortedDict()
                self._view_data[name] = view_data

            if delete:
                to_none = []
                for k, v in view_data.items():
                    to_delete = []
                    for index, d in enumerate(v):
                        if d['id'] == id:
                            to_delete.append(index)
                    if len(v) == len(to_delete):
                        to_none.append(k)
                    else:
                        for index in reversed(to_delete):
                            del v[index]
                for k in to_none:
                    del view_data[k]

            if add:
                r = fn(o)
                if r is None:
                    continue
                for k, v in r.items():
                    current = view_data.get(k)
                    d = {
                        'id': o['_id'],
                        'key': k,
                        'value': v,
                    }
                    if current is None:
                        view_data[k] = [d]
                    else:
                        current.append(d)


class Conflict(Exception):
    pass


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
