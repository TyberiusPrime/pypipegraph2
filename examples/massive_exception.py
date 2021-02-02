import pypipegraph2 as ppg

should_len = 1024 * 1024


def inner(*args):
    # df = pd.DataFrame()
    # df['shu']
    raise ValueError("x " * (should_len // 2))


ppg.FileGeneratingJob("b", lambda of: 5).depends_on(ppg.DataLoadingJob("A", inner))
ppg.run()
#
