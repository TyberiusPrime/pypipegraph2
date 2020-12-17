class Dummy(object):
    pass


from pathlib import Path


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
