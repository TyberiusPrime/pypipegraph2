from pathlib import Path


class Dummy(object):
    pass


def write(filename, text):
    Path(filename).write_text(text)


def append(filename, text):
    p = Path(filename)
    if p.exists():
        old = p.read_text()
    else:
        old = ""
    p.write_text(old + text)


def writeappend(filename_write, filename_append, string):
    write(filename_write, string)
    append(filename_append, string)


def read(filename):
    return Path(filename).read_text()


def counter(filename):
    """Helper for counting invocations in a side-effect file"""
    try:
        res = int(Path(filename).read_text())
    except:  # noqa: E722
        res = 0
    Path(filename).write_text(str(res + 1))
    return str(res)


def force_load(job):
    """Force the loading of a Dataloading job that has no other dependents"""
    import pypipegraph2 as ppg

    ppg.JobGeneratingJob(job.job_id + "_gen", lambda: None).depends_on(job)
