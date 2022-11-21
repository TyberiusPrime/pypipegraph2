import sys
for p in sys.path: print(p)
import pypipegraph2


def test_shu():
    print(dir(pypipegraph2))
    assert pypipegraph2.sum_as_string(7,10) == "17"
