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


def test_save(db):
    o = db.save({'a': 1})
    assert '_id' in o.keys()
    assert o['_id'] is not None
    assert db.has(o['_id'])


def test_get(db):
    o = db.save({'a': 1})
    new_id = o['_id']
    assert new_id is not None
    o = db.get(new_id)
    assert o is not None
    assert o['a'] == 1
    assert '_id' in o.keys()
    assert o['_id'] == new_id
    assert '_rev' in o.keys()


def test_get_2(db):
    o1 = db.save({'a': 1})
    new_id_1 = o1['_id']
    assert new_id_1 is not None
    o2 = db.save({'b': 2})
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
    o = db.save({'a': 1})
    new_id = o['_id']
    assert new_id is not None
    db.delete(new_id)
    assert not db.has(new_id)


def test_update(db):
    o = db.save({'a': 1})
    new_id = o['_id']
    first_rev = o['_rev']
    assert first_rev is not None
    assert new_id is not None
    o['a'] = 2
    o = db.save(o)
    assert o['a'] == 2
    second_rev = o['_rev']
    assert second_rev is not None
    assert first_rev != second_rev
    o = db.get(new_id)
    assert o['a'] == 2
    assert o['_rev'] == second_rev


def test_view_just_save(db):
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    db.save({'a': 2, 'b': 22})
    db.save({'a': 3, 'b': 33})
    db.save({'a': 1, 'b': 11})
    r = db.view('b_by_a')
    r = list(r)
    assert len(r) == 3
    assert r[0] == {'id': 2, 'key': 1, 'value': 11}
    assert r[1] == {'id': 0, 'key': 2, 'value': 22}
    assert r[2] == {'id': 1, 'key': 3, 'value': 33}


def test_view_save_and_update_value(db):
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    db.save({'a': 2, 'b': 22})
    db.save({'a': 3, 'b': 33})
    o1 = db.save({'a': 1, 'b': 11})
    o1['b'] = 1111
    db.save(o1)
    r = db.view('b_by_a')
    r = list(r)
    assert len(r) == 3
    assert r[0] == {'id': 2, 'key': 1, 'value': 1111}
    assert r[1] == {'id': 0, 'key': 2, 'value': 22}
    assert r[2] == {'id': 1, 'key': 3, 'value': 33}


def test_view_save_and_delete(db):
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    o2 = db.save({'a': 2, 'b': 22})
    db.save({'a': 3, 'b': 33})
    db.save({'a': 1, 'b': 11})
    db.delete(o2['_id'])
    r = db.view('b_by_a')
    r = list(r)
    assert len(r) == 2
    assert r[0] == {'id': 2, 'key': 1, 'value': 11}
    assert r[1] == {'id': 1, 'key': 3, 'value': 33}


def test_view_kickstart(db):
    db.save({'a': 2, 'b': 22})
    db.save({'a': 3, 'b': 33})
    db.save({'a': 1, 'b': 11})
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    r = db.view('b_by_a')
    r = list(r)
    assert len(r) == 3
    assert r[0] == {'id': 2, 'key': 1, 'value': 11}
    assert r[1] == {'id': 0, 'key': 2, 'value': 22}
    assert r[2] == {'id': 1, 'key': 3, 'value': 33}


def test_view_by_key(db):
    db.save({'a': 2, 'b': 22})
    db.save({'a': 3, 'b': 33})
    db.save({'a': 1, 'b': 11})
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    r = list(db.view('b_by_a', key=2))
    assert len(r) == 1
    assert r[0] == {'id': 0, 'key': 2, 'value': 22}


def test_view_by_key_string(db):
    db.save({'a': '2', 'b': 22})
    db.save({'a': '3', 'b': 33})
    db.save({'a': '1', 'b': 11})
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    r = list(db.view('b_by_a', key='2'))
    assert len(r) == 1
    assert r[0] == {'id': 0, 'key': '2', 'value': 22}


def test_view_by_key_two_values_same_key_before(db):
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    db.save({'a': 2, 'b': 22})
    db.save({'a': 3, 'b': 33})
    db.save({'a': 1, 'b': 11})
    db.save({'a': 2, 'b': 44})
    r = list(db.view('b_by_a', key=2))
    assert len(r) == 2
    assert r[0] == {'id': 0, 'key': 2, 'value': 22}
    assert r[1] == {'id': 3, 'key': 2, 'value': 44}


def test_view_by_key_two_values_same_key_after(db):
    db.save({'a': 2, 'b': 22})
    db.save({'a': 3, 'b': 33})
    db.save({'a': 1, 'b': 11})
    db.save({'a': 2, 'b': 44})
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    r = list(db.view('b_by_a', key=2))
    assert len(r) == 2
    assert r[0] == {'id': 0, 'key': 2, 'value': 22}
    assert r[1] == {'id': 3, 'key': 2, 'value': 44}


def test_view_by_startkey(db):
    db.save({'a': 2, 'b': 22})
    db.save({'a': 3, 'b': 33})
    db.save({'a': 1, 'b': 11})
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    r = list(db.view('b_by_a', startkey=2))
    assert len(r) == 2
    assert r[0] == {'id': 0, 'key': 2, 'value': 22}
    assert r[1] == {'id': 1, 'key': 3, 'value': 33}


def test_view_by_startkey_after(db):
    db.save({'a': 3, 'b': 33})
    db.save({'a': 4, 'b': 44})
    db.save({'a': 1, 'b': 11})
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    r = list(db.view('b_by_a', startkey=2))
    assert len(r) == 2
    assert r[0] == {'id': 0, 'key': 3, 'value': 33}
    assert r[1] == {'id': 1, 'key': 4, 'value': 44}


def test_view_by_endkey(db):
    db.save({'a': 2, 'b': 22})
    db.save({'a': 3, 'b': 33})
    db.save({'a': 1, 'b': 11})
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    r = list(db.view('b_by_a', endkey=2))
    assert len(r) == 2
    assert r[0] == {'id': 2, 'key': 1, 'value': 11}
    assert r[1] == {'id': 0, 'key': 2, 'value': 22}


def test_view_by_endkey_after(db):
    db.save({'a': 2, 'b': 22})
    db.save({'a': 4, 'b': 44})
    db.save({'a': 1, 'b': 11})
    db.define('b_by_a', lambda o: (o['a'], o['b']))
    r = list(db.view('b_by_a', endkey=3))
    assert len(r) == 2
    assert r[0] == {'id': 2, 'key': 1, 'value': 11}
    assert r[1] == {'id': 0, 'key': 2, 'value': 22}


def test_add_with_custom_keys(db):
    db['a'] = {'a': 2, 'b': 22}
    db[1] = {'a': 3, 'b': 33}
    db[('a', 1)] = {'a': 1, 'b': 11}
    assert db['a'] == {'_id': 'a', '_rev': 0, 'a': 2, 'b': 22}
    assert db[1] == {'_id': 1, '_rev': 0, 'a': 3, 'b': 33}
    assert db[('a', 1)] == {'_id': ['a', 1], '_rev': 0, 'a': 1, 'b': 11}


def test_add_with_custom_keys_and_set_next_id(db):
    db[10] = {'a': 3, 'b': 33}
    db.set_next_id(20)
    db[None] = {'a': 1, 'b': 11}
    assert db[10] == {'_id': 10, '_rev': 0, 'a': 3, 'b': 33}
    assert db[20] == {'_id': 20, '_rev': 0, 'a': 1, 'b': 11}


def test_include_docs(db):
    db.define('by_id', lambda o: (o['_id'], 1))
    db[1] = {1: 11}
    db[2] = {2: 12}
    db[5] = {5: 15}
    db[7] = {7: 17}
    r = list(db.view('by_id', include_docs=True))
    assert r[0] == {'id': 1, 'key': 1, 'value': 1,
                    'doc': {'_id': 1, '_rev': 0, '1': 11}}
    assert r[1] == {'id': 2, 'key': 2, 'value': 1,
                    'doc': {'_id': 2, '_rev': 0, '2': 12}}
    assert r[2] == {'id': 5, 'key': 5, 'value': 1,
                    'doc': {'_id': 5, '_rev': 0, '5': 15}}
    assert r[3] == {'id': 7, 'key': 7, 'value': 1,
                    'doc': {'_id': 7, '_rev': 0, '7': 17}}


def test_yielding_mapping_function(db):
    def yielder(o):
        yield (o['a'], 1), o['b']
        yield (o['a'], 2), o['b'] * 2
        yield (o['a'], 3), o['b'] * 3

    db.save({'a': 2, 'b': 22})
    db.save({'a': 3, 'b': 33})
    db.save({'a': 1, 'b': 11})
    db.define('b_by_a', yielder)
    r = db.view('b_by_a')
    r = list(r)
    assert len(r) == 9
    assert r[0] == {'id': 2, 'key': (1, 1), 'value': 11}
    assert r[1] == {'id': 2, 'key': (1, 2), 'value': 22}
    assert r[2] == {'id': 2, 'key': (1, 3), 'value': 33}
    assert r[3] == {'id': 0, 'key': (2, 1), 'value': 22}
    assert r[4] == {'id': 0, 'key': (2, 2), 'value': 44}
    assert r[5] == {'id': 0, 'key': (2, 3), 'value': 66}
    assert r[6] == {'id': 1, 'key': (3, 1), 'value': 33}
    assert r[7] == {'id': 1, 'key': (3, 2), 'value': 66}
    assert r[8] == {'id': 1, 'key': (3, 3), 'value': 99}


def test_reduce_by_group(db):
    def sum_per(field, values):
        result = {}
        for value in values:
            v = value.get(field)
            if v in result:
                result[v] += 1
            else:
                result[v] = 1
        return result

    db.define('test',
              lambda o: (o['category'], {'state': o['state']}),
              lambda keys, values, rereduce: sum_per('state', values))
    db.save({'category': 'a', 'state': 'new'})
    db.save({'category': 'b', 'state': 'new'})
    db.save({'category': 'a', 'state': 'old'})
    db.save({'category': 'b', 'state': 'new'})
    db.save({'category': 'a', 'state': 'old'})
    db.save({'category': 'a', 'state': 'new'})
    db.save({'category': 'c', 'state': 'new'})
    db.save({'category': 'c', 'state': 'old'})
    db.save({'category': 'a', 'state': 'new'})
    db.save({'category': 'a', 'state': 'new'})
    r = list(db.view('test', group=True))
    print(r)
    assert r[0] == {'key': 'a', 'value': {'new': 4, 'old': 2}}
    assert r[1] == {'key': 'b', 'value': {'new': 2}}
    assert r[2] == {'key': 'c', 'value': {'new': 1, 'old': 1}}


def test_skip(db):
    db.define('by_id', lambda o: (o['_id'], 1))
    db[1] = {1: 11}
    db[2] = {2: 12}
    db[5] = {5: 15}
    db[7] = {7: 17}
    r = list(db.view('by_id', include_docs=True, skip=2))
    assert r[0] == {'id': 5, 'key': 5, 'value': 1,
                    'doc': {'_id': 5, '_rev': 0, '5': 15}}
    assert r[1] == {'id': 7, 'key': 7, 'value': 1,
                    'doc': {'_id': 7, '_rev': 0, '7': 17}}


def test_limit(db):
    db.define('by_id', lambda o: (o['_id'], 1))
    db[1] = {1: 11}
    db[2] = {2: 12}
    db[5] = {5: 15}
    db[7] = {7: 17}
    r = list(db.view('by_id', include_docs=True, limit=2))
    assert r[0] == {'id': 1, 'key': 1, 'value': 1,
                    'doc': {'_id': 1, '_rev': 0, '1': 11}}
    assert r[1] == {'id': 2, 'key': 2, 'value': 1,
                    'doc': {'_id': 2, '_rev': 0, '2': 12}}


def test_skip_and_limit(db):
    db.define('by_id', lambda o: (o['_id'], 1))
    db[1] = {1: 11}
    db[2] = {2: 12}
    db[5] = {5: 15}
    db[7] = {7: 17}
    r = list(db.view('by_id', include_docs=True, skip=1, limit=2))
    assert r[0] == {'id': 2, 'key': 2, 'value': 1,
                    'doc': {'_id': 2, '_rev': 0, '2': 12}}
    assert r[1] == {'id': 5, 'key': 5, 'value': 1,
                    'doc': {'_id': 5, '_rev': 0, '5': 15}}
