# -*- coding: utf-8 -*-
"""
    Dummy conftest.py for pypipegraph2.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""

# import pytest
import pytest
from pathlib import Path
import shutil
import os
import pypipegraph2 as ppg
import sys
from loguru import logger

# support code to remove test created files
# only if the test suceeded
# ppg.util._running_inside_test = True
if "pytest" not in sys.modules:
    raise ValueError("fixtures can only be used together with pytest")


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture
def ppg_per_test(request):
    import sys

    if request.cls is None:
        target_path = Path(request.fspath).parent / "run" / ("." + request.node.name)
    else:
        target_path = (
            Path(request.fspath).parent
            / "run"
            / (request.cls.__name__ + "." + request.node.name)
        )
        target_path = target_path.absolute()
    old_dir = Path(os.getcwd()).absolute()
    if old_dir == target_path:
        pass
    else:
        if target_path.exists():  # pragma: no cover
            shutil.rmtree(target_path)

    try:
        first = [False]

        def np(quiet=True, **kwargs):
            if not first[0]:
                Path(target_path).mkdir(parents=True, exist_ok=True)
                os.chdir(target_path)
                first[0] = True

            g = ppg.new(
                cores=1,
                # log_level=5,
                allow_short_filenames=True,
                run_mode=ppg.RunMode.NONINTERACTIVE,
            )
            g.new = np
            return g

        def finalize():
            if hasattr(request.node, "rep_setup"):

                if request.node.rep_setup.passed and (
                    request.node.rep_call.passed
                    or request.node.rep_call.outcome == "skipped"
                ):
                    try:
                        # if not hasattr(ppg.util.global_pipegraph, "test_keep_output"):
                        if "--profile" not in sys.argv:
                            shutil.rmtree(target_path)
                    except OSError:  # pragma: no cover
                        pass

        request.addfinalizer(finalize)
        yield np()

    finally:
        os.chdir(old_dir)


@pytest.fixture
def dir_per_test(request):
    """No pipegraph, but seperate directory per test"""
    if request.cls is None:
        target_path = Path(request.fspath).parent / "run" / ("." + request.node.name)
    else:
        target_path = (
            Path(request.fspath).parent
            / "run"
            / (request.cls.__name__ + "." + request.node.name)
        )
    if target_path.exists():  # pragma: no cover
        shutil.rmtree(target_path)
    target_path = target_path.absolute()
    target_path.mkdir()
    old_dir = Path(os.getcwd()).absolute()
    os.chdir(target_path)
    try:

        def np():
            ppg.util.global_pipegraph = None
            return None

        def finalize():
            if hasattr(request.node, "rep_setup"):

                if request.node.rep_setup.passed and (
                    request.node.rep_call.passed
                    or request.node.rep_call.outcome == "skipped"
                ):
                    try:
                        shutil.rmtree(target_path)
                    except OSError:  # pragma: no cover
                        pass

        request.addfinalizer(finalize)
        ppg.util.global_pipegraph = None
        yield np()

    finally:
        os.chdir(old_dir)


@pytest.fixture
def create_out_dir(request):
    Path("out").mkdir()
    yield


@pytest.fixture
def both_ppg_and_no_ppg(request):
    """Create both an inside and an outside ppg test case.
    don't forgot to add this to your conftest.py

    Use togother with run_ppg and force_load

    ```
    def pytest_generate_tests(metafunc):
        if "both_ppg_and_no_ppg" in metafunc.fixturenames:
            metafunc.parametrize("both_ppg_and_no_ppg", [True, False], indirect=True)
    ```
    """
    raise ValueError("check implemenattion")

    if request.param:
        if request.cls is None:
            target_path = (
                Path(request.fspath).parent
                / "run"
                / ("." + request.node.name + str(request.param))
            )
        else:
            target_path = (
                Path(request.fspath).parent
                / "run"
                / (request.cls.__name__ + "." + request.node.name)
            )
        if target_path.exists():  # pragma: no cover
            shutil.rmtree(target_path)
        target_path = target_path.absolute()
        old_dir = Path(os.getcwd()).absolute()
        try:
            first = [False]

            def np():
                if not first[0]:
                    Path(target_path).mkdir(parents=True, exist_ok=True)
                    os.chdir(target_path)
                    first[0] = True

                rc = ppg.resource_coordinators.LocalSystem()
                ppg.new_pipegraph(rc, quiet=True, dump_graph=False)
                ppg.util.global_pipegraph.result_dir = Path("results")
                g = ppg.util.global_pipegraph
                g.new_pipegraph = np
                return g

            def finalize():
                if hasattr(request.node, "rep_setup"):

                    if request.node.rep_setup.passed and (
                        hasattr(request.node, "rep_call")
                        and (
                            request.node.rep_call.passed
                            or request.node.rep_call.outcome == "skipped"
                        )
                    ):
                        try:
                            shutil.rmtree(target_path)
                        except OSError:  # pragma: no cover
                            pass

            request.addfinalizer(finalize)
            yield np()

        finally:
            os.chdir(old_dir)
    else:
        if request.cls is None:
            target_path = (
                Path(request.fspath).parent
                / "run"
                / ("." + request.node.name + str(request.param))
            )
        else:
            target_path = (
                Path(request.fspath).parent
                / "run"
                / (request.cls.__name__ + "." + request.node.name)
            )
        if target_path.exists():  # pragma: no cover
            shutil.rmtree(target_path)
        target_path = target_path.absolute()
        target_path.mkdir()
        old_dir = Path(os.getcwd()).absolute()
        os.chdir(target_path)
        try:

            def np():
                ppg.util.global_pipegraph = None

                class Dummy:
                    pass

                d = Dummy
                d.new_pipegraph = lambda: None
                return d

            def finalize():
                if hasattr(request.node, "rep_setup"):

                    if request.node.rep_setup.passed and (
                        request.node.rep_call.passed
                        or request.node.rep_call.outcome == "skipped"
                    ):
                        try:
                            shutil.rmtree(target_path)
                        except OSError:  # pragma: no cover
                            pass

            request.addfinalizer(finalize)
            ppg.util.global_pipegraph = None
            yield np()

        finally:
            os.chdir(old_dir)


@pytest.fixture
def job_trace_log():
    def fmt(record):
        lvl = str(record["level"].name).ljust(8)
        m = record["module"] + ":"
        func = f"{m:12}{record['line']:4}"
        func = func.ljust(12 + 4)
        out = f"{record['level'].icon} {lvl} | {func} | {record['message']}\n"
        if record["level"].name == "ERROR":
            out = f"<blue>{out}</blue>"
        return out

    logger.remove()
    handler_id = logger.add(sys.stderr, format=fmt, level=6)
    yield
    logger.remove(handler_id)


@pytest.fixture
def trace_log():  # could not find out how to abstract pytest fixtures
    def fmt(record):
        lvl = str(record["level"].name).ljust(8)
        m = record["module"] + ":"
        func = f"{m:12}{record['line']:4}"
        func = func.ljust(12 + 4)
        out = f"{record['level'].icon} {lvl} | {func} | {record['message']}\n"
        if record["level"].name == "ERROR":
            out = f"<blue>{out}</blue>"
        return out

    logger.remove()  # no coming back after this :(
    handler_id = logger.add(sys.stderr, format=fmt, level=5)
    yield
    logger.remove(handler_id)
    # logger.add(old)
