.. image:: https://travis-ci.org/eblade/jsonobject.svg?branch=master
    :target: https://travis-ci.org/eblade/jsondb


jsondb
======

JSON Key-Value store in pure Python 3

Introduction
------------

JSONDB is a library for Python 3 that provides the ability to run a very
simplified CouchDB-like document database, a.k.a. a Key-Value store. The
features include:

- Hard disk storage of documents
- In-memory storage of indexes
- Map and reduce functions specified in Python directly
- Any number of views per database
- Views can be accessed with or without reducing them
- Thread-safe (with locks per database)


Installation
------------

You can `pip` (python 3) install this Github repository or a tag, like this:

    $pip install https://github.com/eblade/jsondb/archive/0.2.tar.gz


This will also install `blist` which is used to get the views faster.


Examples
--------

To create a new database (a table if you think in relation database terms):


.. code:: python

    >>> from jsondb import Database
    >>> db = Database('/tmp/cars')
    >>> db.clear() # for doctest purposes


This will create a folder `/tmp/cars` which will be used to store the
documents (json files) and an ID counter.

To populated the database with some content you can use ``db.save(...)``.
These documents will be given a unique ``id`` automatically. If you just
want to retrieve them using indices, this is not a problem, but if you
want control over the identifiers, you can do like this instead:


.. code:: python

    >>> db[0] = {'brand': 'Volvo', 'model': 'S40', 'wheels': 6}
    >>> db[1] = {'brand': 'Mercedes', 'model': 'C', 'wheels': 8}
    >>> db[2] = {'brand': 'Volvo', 'model': 'V70', 'wheels': 4}
    >>> db[3] = {'brand': 'Honda', 'model': 'CB500F', 'wheels': 2}


This enables you to retrieve them back in the expected pythonic way.

The documents are stored synchronously, so your app may be restarted
without data loss.

Let's look at an interactive session to find out what the document
looks like when it comes back:


.. code-block:: python

    >>> db[0] == {'wheels': 6, '_id': 0, '_rev': 0, 'brand': 'Volvo', 'model': 'S40'}
    True


As you can see, the structure closely mimic that of CouchDB, with the
``_id`` and ``_rev`` fields. The ``_rev`` field is important to keep intact
as updated requires it to be the latest (otherwise a ``jsondb.Conflict``
is raised). To update, it's quite easy to use save (but index-based
setting also works):


.. code-block:: python

    >>> db.save({'wheels': 6, '_id': 0, '_rev': 0, 'brand': 'Volvo', 'model': 'S40', 'color': 'white'}) == \
    ... {'wheels': 6, '_id': 0, '_rev': 1, 'brand': 'Volvo', 'model': 'S40', 'color': 'white'}
    True


The ``_rev`` should change here, usually pop one number up (whereas
CouchDB would return random hashes for each revision).

To delete a document you can simple use ``del db[key]`` or
``db.delete(key)``.


Views
~~~~~

What fun is a Key-Value store with no indexing? Not much!

.. code:: python

    >>> db.define('by_wheels', lambda o: (o['wheels'], ' '.join([o['brand'], o['model']])))
    >>> list(db.view('by_wheels'))[0] == \
    ... {'id': 3, 'key': 2, 'value': 'Honda CB500F'}
    True


So we defined a view called ``by_wheels`` where the number of wheels
is used as key and a concatenation of brand and model is used as
value. The view is always sorted so I know that the motorcycle will
come out first. The rest of the order is somewhat arbitrary since
a binary search tree is used to hold the index in memory.

Note that the index is available as soon as it is created. This is
because the operation of defining an index is asynchronous. It does
not matter if the view is defined before or after the documents are
created, as the documents will be placed in the index ad hoc. They
will also be deleted that way. This means, for performance:

- Adding a document is O(log n)
- Finding a document is O(log n)
- Deleting a document is O(log n)

So this scales quite well as long as the index fits in memory (the
actual documents do not need to fit in memory, however). By the nature
of being a binary search tree, it is constantly sorted by key.

Now, this takes us to the sorting. To further mimic CouchDB, keys need
to be sortable beyond the core functionality of python. Anything needs
to be comparable with anything basically. Also, we need something to
be smaller and bigger than everything else, respectively. These are
``None`` and ``any``.

Lets revisit the ``by_wheels`` view, and take everything with equal to
or more than 6 wheels (I know this is not accurate data).

.. code:: python

    >>> list(db.view('by_wheels', startkey=6, endkey=any)) == \
    ... [{'id': 0, 'key': 6, 'value': 'Volvo S40'},{'id': 1, 'key': 8, 'value': 'Mercedes C'}]
    True

The reason to use ``list()`` here is because I'm always given a
generator back.


More on Views
~~~~~~~~~~~~~

A number of keyword arguments can be passed to the ``view(...)`` method:

- ``key`` specifies a single key (which can give 0 to many values)
- ``startkey`` specifies an inclusive starting point. Can be a tuple.
- ``endkey`` specifies and inclusive ending point. Can be a tuple.
- ``include_docs``, if ``True``, the document that rendered this index
  post is included under ``doc``.
- ``group``, if ``True`` and a ``reduce`` function is specified as a
  third argument to the ``define`` method, the result will be the
  reduced data rather than the mapped.
- ``no_reduce``, if there is a reduce function, but you don't want to
  use it this time, set this to ``True`` and leave ``group`` as
  ``False``.
- ``skip``, an integer offset (defaults to ``0``)
- ``limit``, an integer page size (set to ``None`` for no limit)


For more information about reduce functions please see the CouchDB
documentation. The big differences are:

- Group levels are not supported. Grouping is always done on the deepest
  level (meaning all elements in a tuple key).
- Re-reduce is never done. But. The reduce function nevertheless expects
  ``f(keys, values, rereduce)``. This potentially leads to scaling
  issues but I have not run into them yet.


Further Reading
---------------

- The lib is developed mainly for the Images6 project, found at
  https://github.com/eblade/images6. This means it's full of usage
  examples. Look into ``images6/system.py`` for instance to see how
  the views are set up.

- Also the lib works quite well together with its sister, ``jsonobject``
  which is a Django-inspired serialization/deserialization lib for
  complex python objects and json. It can be found here:
  https://github.com/eblade/jsonobject.


Author
------

``jsondb`` is written and maintained by Johan Egneblad <johan@egneblad.se>.
