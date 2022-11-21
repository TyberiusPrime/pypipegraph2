import sys
for p in sys.path: print(p)
import ppg2_rust


def test_shu():
    print(dir(ppg2_rust))
    assert ppg2_rust.sum_as_string(7,10) == "17"
