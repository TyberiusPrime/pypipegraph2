import os

cpu_count = None


def escape_logging(s):
    return str(s).replace("<", "\\<").replace("{", "{{").replace("}", "}}")


def CPUs():
    """
    Detects the number of CPUs on a system. Cribbed from pp.
    """
    global cpu_count
    if cpu_count == None:
        cpu_count = 1  # default
        # Linux, Unix and MacOS:
        if hasattr(os, "sysconf"):
            if "SC_NPROCESSORS_ONLN" in os.sysconf_names:
                # Linux & Unix:
                ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
                if isinstance(ncpus, int) and ncpus > 0:
                    cpu_count = ncpus
            else:  # OSX: pragma: no cover
                cpu_count = int(
                    os.popen2("sysctl -n hw.ncpu")[1].read()
                )  # pragma: no cover
        # Windows:
        if "NUMBER_OF_PROCESSORS" in os.environ:  # pragma: no cover
            ncpus = int(os.environ["NUMBER_OF_PROCESSORS"])
            if ncpus > 0:
                cpu_count = ncpus
    return cpu_count
