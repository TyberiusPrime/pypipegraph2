import pytest
from pathlib import Path


def read(filename):
    """simply read a file"""
    op = open(filename)
    data = op.read()
    op.close()
    return data


def write(filename, string):
    """open file for writing, dump string, close file"""
    op = open(Path(filename).absolute(), "w")
    op.write(string)
    op.close()


def append(filename, string):
    """open file for appending, dump string, close file"""
    op = open(filename, "a")
    op.write(string)
    op.close()


def writeappend(filename_write, filename_append, string):
    write(filename_write, string)
    append(filename_append, string)


def assertRaises(exception, func):
    with pytest.raises(exception):
        func()


class Dummy(object):
    pass


def force_load(job):
    """Force the loading of a conditional job
    (Dataloading, AttributeLoading, TempFile...) job
    that has no other dependents"""
    import pypipegraph2 as ppg

    ppg.JobGeneratingJob(job.job_id + "_gen", lambda: None).depends_on(job)
