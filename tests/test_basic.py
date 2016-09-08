import pytest
import logging
import tempfile
import jsondb


# Logging
FORMAT = '%(asctime)s [%(threadName)s] %(filename)s +%(levelno)s ' + \
         '%(funcName)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)


@pytest.fixture(scope='function')
def db():
    db = jsondb.Database(root=tempfile.mkdtemp(prefix='jsondb-'))
    yield db
    db.destroy()


def test_init(db):
    assert db is not None


def test_put(db):
    o = db.put({'a': 1})
    assert '_id' in o.keys()
    assert o['_id'] is not None
    assert db.has(o['_id'])


def test_get(db):
    o = db.put({'a': 1})
    new_id = o['_id']
    assert new_id is not None
    o = db.get(new_id)
    assert o is not None
    assert o['a'] == 1
    assert '_id' in o.keys()
    assert o['_id'] == new_id
    assert '_rev' in o.keys()


def test_get_2(db):
    o1 = db.put({'a': 1})
    new_id_1 = o1['_id']
    assert new_id_1 is not None
    o2 = db.put({'b': 2})
    new_id_2 = o2['_id']
    assert new_id_2 is not None
    o1 = db.get(new_id_1)
    assert o1 is not None
    assert o1['a'] == 1
    assert '_id' in o1.keys()
    assert o1['_id'] == new_id_1
    assert '_rev' in o1.keys()
    o2 = db.get(new_id_2)
    assert o2 is not None
    assert o2['b'] == 2
    assert '_id' in o2.keys()
    assert o2['_id'] == new_id_2
    assert '_rev' in o2.keys()


def test_delete(db):
    o = db.put({'a': 1})
    new_id = o['_id']
    assert new_id is not None
    db.delete(new_id)
    assert not db.has(new_id)


def test_update(db):
    o = db.put({'a': 1})
    new_id = o['_id']
    first_rev = o['_rev']
    assert first_rev is not None
    assert new_id is not None
    o['a'] = 2
    o = db.update(o)
    assert o['a'] == 2
    second_rev = o['_rev']
    assert second_rev is not None
    assert first_rev != second_rev
    o = db.get(new_id)
    assert o['a'] == 2
    assert o['_rev'] == second_rev


def test_view_just_put(db):
    db.define('b_by_a', lambda o: {o['a']: o['b']})
    db.put({'a': 2, 'b': 22})
    db.put({'a': 3, 'b': 33})
    db.put({'a': 1, 'b': 11})
    r = db.view('b_by_a')
    r = list(r)
    assert len(r) == 3
    assert r[0] == {'id': 2, 'key': 1, 'value': 11}
    assert r[1] == {'id': 0, 'key': 2, 'value': 22}
    assert r[2] == {'id': 1, 'key': 3, 'value': 33}


def test_view_put_and_update_value(db):
    db.define('b_by_a', lambda o: {o['a']: o['b']})
    db.put({'a': 2, 'b': 22})
    db.put({'a': 3, 'b': 33})
    o1 = db.put({'a': 1, 'b': 11})
    o1['b'] = 1111
    db.update(o1)
    r = db.view('b_by_a')
    r = list(r)
    assert len(r) == 3
    assert r[0] == {'id': 2, 'key': 1, 'value': 1111}
    assert r[1] == {'id': 0, 'key': 2, 'value': 22}
    assert r[2] == {'id': 1, 'key': 3, 'value': 33}


def test_view_put_and_delete(db):
    db.define('b_by_a', lambda o: {o['a']: o['b']})
    o2 = db.put({'a': 2, 'b': 22})
    db.put({'a': 3, 'b': 33})
    db.put({'a': 1, 'b': 11})
    db.delete(o2['_id'])
    r = db.view('b_by_a')
    r = list(r)
    assert len(r) == 2
    assert r[0] == {'id': 2, 'key': 1, 'value': 11}
    assert r[1] == {'id': 1, 'key': 3, 'value': 33}


def test_view_kickstart(db):
    db.put({'a': 2, 'b': 22})
    db.put({'a': 3, 'b': 33})
    db.put({'a': 1, 'b': 11})
    db.define('b_by_a', lambda o: {o['a']: o['b']})
    r = db.view('b_by_a')
    r = list(r)
    assert len(r) == 3
    assert r[0] == {'id': 2, 'key': 1, 'value': 11}
    assert r[1] == {'id': 0, 'key': 2, 'value': 22}
    assert r[2] == {'id': 1, 'key': 3, 'value': 33}


def test_view_by_key(db):
    db.put({'a': 2, 'b': 22})
    db.put({'a': 3, 'b': 33})
    db.put({'a': 1, 'b': 11})
    db.define('b_by_a', lambda o: {o['a']: o['b']})
    r = list(db.view('b_by_a', key=2))
    assert len(r) == 1
    assert r[0] == {'id': 0, 'key': 2, 'value': 22}


def test_view_by_key_two_values_same_key_before(db):
    db.define('b_by_a', lambda o: {o['a']: o['b']})
    db.put({'a': 2, 'b': 22})
    db.put({'a': 3, 'b': 33})
    db.put({'a': 1, 'b': 11})
    db.put({'a': 2, 'b': 44})
    r = list(db.view('b_by_a', key=2))
    assert len(r) == 2
    assert r[0] == {'id': 0, 'key': 2, 'value': 22}
    assert r[1] == {'id': 3, 'key': 2, 'value': 44}


def test_view_by_key_two_values_same_key_after(db):
    db.put({'a': 2, 'b': 22})
    db.put({'a': 3, 'b': 33})
    db.put({'a': 1, 'b': 11})
    db.put({'a': 2, 'b': 44})
    db.define('b_by_a', lambda o: {o['a']: o['b']})
    r = list(db.view('b_by_a', key=2))
    assert len(r) == 2
    assert r[0] == {'id': 0, 'key': 2, 'value': 22}
    assert r[1] == {'id': 3, 'key': 2, 'value': 44}


def test_view_by_startkey(db):
    db.put({'a': 2, 'b': 22})
    db.put({'a': 3, 'b': 33})
    db.put({'a': 1, 'b': 11})
    db.define('b_by_a', lambda o: {o['a']: o['b']})
    r = list(db.view('b_by_a', startkey=2))
    assert len(r) == 2
    assert r[0] == {'id': 0, 'key': 2, 'value': 22}
    assert r[1] == {'id': 1, 'key': 3, 'value': 33}


def test_view_by_endkey(db):
    db.put({'a': 2, 'b': 22})
    db.put({'a': 3, 'b': 33})
    db.put({'a': 1, 'b': 11})
    db.define('b_by_a', lambda o: {o['a']: o['b']})
    r = list(db.view('b_by_a', endkey=2))
    assert len(r) == 2
    assert r[0] == {'id': 2, 'key': 1, 'value': 11}
    assert r[1] == {'id': 0, 'key': 2, 'value': 22}
