from jsondb import csv


def test_read_csv():
    with csv.open('tests/data/simple.csv', types=(str, int, int)) as f:
        rows = list(f)

    assert rows[0] == {'a': 'A', 'b': 1, 'c': 11}
    assert rows[1] == {'a': 'B', 'b': 2, 'c': 22}
    assert rows[2] == {'a': 'C', 'b': 3, 'c': 33}


def test_lookup_by_method():
    with csv.open('tests/data/simple.csv', types=(str, int, int)) as f:
        lt = csv.LookupTable(f)

    lt.index('a', 'b')
    lt.index('b', 'c')

    assert lt['a2b':'A'] == 1
    assert lt['a2b':'B'] == 2
    assert lt['a2b':'C'] == 3
    assert lt['b2c':1] == 11
    assert lt['b2c':2] == 22
    assert lt['b2c':3] == 33


def test_lookup_by_argument():
    with csv.open('tests/data/simple.csv', types=(str, int, int)) as f:
        lt = csv.LookupTable(f, ('a', 'b'), ('b', 'c'))

    assert lt['a2b':'A'] == 1
    assert lt['a2b':'B'] == 2
    assert lt['a2b':'C'] == 3
    assert lt['b2c':1] == 11
    assert lt['b2c':2] == 22
    assert lt['b2c':3] == 33
