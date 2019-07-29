import csv
_open = open


class open:
    def __init__(self, filename, types=None, encoding='utf8', **opts):
        self.filename = filename
        self.types = types
        self.encoding = encoding
        self.opts = opts

    def __enter__(self):
        self.fp = _open(self.filename, 'rt', encoding=self.encoding)
        self.dialect = csv.Sniffer().sniff(self.fp.read(1024))
        self.fp.seek(0)
        return self

    def __iter__(self):
        reader = csv.reader(self.fp, self.dialect, **self.opts)
        first = True
        for row in reader:
            if first:
                headings = row
                first = False
            else:
                if self.types is None:
                    yield dict(zip(headings, row))
                else:
                    yield dict(zip(
                        headings,
                        (
                            type(value) for type, value
                            in zip(self.types, row)
                        )
                    ))

    def __exit__(self, type, value, tb):
        self.fp.close()


class LookupTable:
    def __init__(self, csv, *indices, **aliases):
        self.data = list(csv)
        self._index = {}
        if aliases:
            for row in self.data:
                for alias, fn in aliases.items():
                    row[alias] = fn(row)
        for index in indices:
            self.index(*index)

    def index(self, key, value):
        index_name = '%s2%s' % (str(key), str(value))
        self._index[index_name] = {
            row[key]: row[value] for row in self.data
        }
        return index_name

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self._index[key.start].get(key.stop)
        else:
            return self._index[key]
