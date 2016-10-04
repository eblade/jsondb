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
    def __init__(self, csv, *indices):
        self.data = list(csv)
        self._index = {}
        for index in indices:
            self.index(*index)

    def index(self, key, value):
        self._index[key] = {
            row[key]: row[value] for row in self.data
        }

    def __getitem__(self, key):
        return self._index[key.start].get(key.stop)
