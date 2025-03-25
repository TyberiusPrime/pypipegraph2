import sys
sys.path.append('python')
import pypipegraph2 as ppg


class FakeRunner:
    def __init__(self):
        self.stat_cache = {}
        self._hash_file_cache = {}

ppg.new(run_mode=ppg.enums.RunMode.NOTEBOOK)

runner = FakeRunner
a = 5

def one():
    return a + 5


af1 = ppg.FunctionInvariant("one", one)
a = 10
def one():
    return a + 5


af2 = ppg.FunctionInvariant("onex", one)


def two():
    return a + 5


bf1 = ppg.FunctionInvariant("two", two)

a = 15
bf2 = ppg.FunctionInvariant("two", two)

print('af1', af1.run(runner, {})['FIone']['3.11'])
print('af2', af2.run(runner, {})['FIonex']['3.11'])
print('bf1', bf1.run(runner, {})['FItwo']['3.11'])
print('bf2', bf2.run(runner, {})['FItwo']['3.11'])

print(af1.function())
print(af2.function())
