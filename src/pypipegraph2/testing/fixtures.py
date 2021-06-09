import sys
import pytest
import os
import shutil

from pathlib import Path
import pypipegraph2 as ppg2
import pypipegraph2.ppg1_compatibility

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
def new_pipegraph(request):
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
                Path("cache").mkdir()
                Path("results").mkdir()
                Path("out").mkdir()

                first[0] = True

            g = ppg2.new(
                cores=1,
                # log_level=5,
                allow_short_filenames=True,
                run_mode=ppg2.RunMode.NONINTERACTIVE,
                log_level = 40
            )
            g.new = np
            g.new_pipegraph = g.new # ppg1 test case compatibility
            g.result_dir = Path('results') # ppg test case compatibility
            if ppg2.ppg1_compatibility.patched:
                g.rc = ppg2.ppg1_compatibility.FakeRC()
            return g

        def finalize():
            if hasattr(request.node, "rep_setup"):

                if request.node.rep_setup.passed and (
                    request.node.rep_call.passed
                    or request.node.rep_call.outcome == "skipped"
                ):
                    try:
                        # if not hasattr(ppg2.util.global_pipegraph, "test_keep_output"):
                        if "--profile" not in sys.argv:
                            shutil.rmtree(target_path)
                    except OSError:  # pragma: no cover
                        pass

        request.addfinalizer(finalize)
        yield np()

    finally:
        os.chdir(old_dir)





@pytest.fixture
def no_pipegraph(request):
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
            ppg2.global_pipegraph = None
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
        ppg2.global_pipegraph = None
        yield np()

    finally:
        os.chdir(old_dir)


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

            def np(quiet=True, **kwargs):
                if not first[0]:
                    Path(target_path).mkdir(parents=True, exist_ok=True)
                    os.chdir(target_path)
                    Path("cache").mkdir()
                    Path("results").mkdir()
                    Path("out").mkdir()

                    first[0] = True

                g = ppg2.new(
                    cores=1,
                    # log_level=5,
                    allow_short_filenames=True,
                    run_mode=ppg2.RunMode.NONINTERACTIVE,
                    log_level = 40
                )
                g.new = np
                g.new_pipegraph = np # ppg1 test case compatibility
                g.result_dir = Path('results') # ppg test case compatibility
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
                ppg2.global_pipegraph = None

                class Dummy:
                    pass

                d = Dummy
                d.new = lambda: None
                d.new_pipegraph = lambda: None # ppg test case compatibility
                d.result_dir = Path('results') # ppg test case compatibility
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
            ppg2.global_pipegraph = None
            yield np()

        finally:
            os.chdir(old_dir)
