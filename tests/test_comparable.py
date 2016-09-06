import pytest
from jsondb import Comparable


@pytest.mark.parametrize('a,b,expected', [
    ('a', 'b', True),
    ('b', 'a', False),
    ('a', 'a', False),
    ('a', None, False),
    (None, 'a', True),
    ('a', any, True),
    (any, 'a', False),
    (None, None, False),
    (any, any, False),
    (1, 1, False),
    (1, 2, True),
    (2, 1, False),
    (0, None, False),
    (None, 0, True),
    (any, 0, False),
    (0, any, True),
    (1, 'a', True),
    ('1', 'a', True),
    ('a', '1', False),
    ('a', 1, False),
])
def test_less_than(a, b, expected):
    a = Comparable(a)
    b = Comparable(b)
    assert (a < b) is expected


@pytest.mark.parametrize('a,b,expected', [
    ('a', 'b', False),
    ('b', 'a', True),
    ('a', 'a', False),
    ('a', None, True),
    (None, 'a', False),
    ('a', any, False),
    (any, 'a', True),
    (None, None, False),
    (any, any, False),
    (1, 1, False),
    (1, 2, False),
    (2, 1, True),
    (0, None, True),
    (None, 0, False),
    (any, 0, True),
    (0, any, False),
    (1, 'a', False),
    ('1', 'a', False),
    ('a', '1', True),
    ('a', 1, True),
])
def test_greater_than(a, b, expected):
    a = Comparable(a)
    b = Comparable(b)
    assert (a > b) is expected


@pytest.mark.parametrize('a,b,expected', [
    ('a', 'b', True),
    ('b', 'a', False),
    ('a', 'a', True),
    ('a', None, False),
    (None, 'a', True),
    ('a', any, True),
    (any, 'a', False),
    (None, None, True),
    (any, any, True),
    (1, 1, True),
    (1, 2, True),
    (2, 1, False),
    (0, None, False),
    (None, 0, True),
    (any, 0, False),
    (0, any, True),
    (1, 'a', True),
    ('1', 'a', True),
    ('a', '1', False),
    ('a', 1, False),
])
def test_less_or_equal(a, b, expected):
    a = Comparable(a)
    b = Comparable(b)
    assert (a <= b) is expected


@pytest.mark.parametrize('a,b,expected', [
    ('a', 'b', False),
    ('b', 'a', True),
    ('a', 'a', True),
    ('a', None, True),
    (None, 'a', False),
    ('a', any, False),
    (any, 'a', True),
    (None, None, True),
    (any, any, True),
    (1, 1, True),
    (1, 2, False),
    (2, 1, True),
    (0, None, True),
    (None, 0, False),
    (any, 0, True),
    (0, any, False),
    (1, 'a', False),
    ('1', 'a', False),
    ('a', '1', True),
    ('a', 1, True),
])
def test_greater_or_equal(a, b, expected):
    a = Comparable(a)
    b = Comparable(b)
    assert (a >= b) is expected
