import pytest
import pypipegraph2 as ppg


@pytest.mark.usefixtures("ppg2_per_test")
class TestUtils:
    def test_assert_uniqueness_simple(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")

        with pytest.raises(ValueError):
            Dummy("shu")

    def test_assert_uniqueness_ok(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")
        Dummy("sha")

        with pytest.raises(ValueError):
            Dummy("shu")

    def test_assert_uniqueness_ok_multi_classes(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")
        Dummy2("shu")

        with pytest.raises(ValueError):
            Dummy("shu")

    def test_assert_uniqueness_raises_slashes(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        Dummy("shu")

        with pytest.raises(ValueError):
            Dummy("shu/sha")

    def test_assert_uniqueness_raises_also_check(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self, also_check=Dummy)

        Dummy("shu")

        with pytest.raises(ValueError):
            Dummy2("shu")

    def test_assert_uniqueness_raises_also_check_no_instance_of_second_class(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self, also_check=Dummy)

        # a = Dummy('shu')
        # does not raise of course...
        Dummy2("shu")

        with pytest.raises(ValueError):
            Dummy2("shu")

    def test_assert_uniqueness_raises_also_check_list(self):
        class Dummy:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self)

        class Dummy2:
            def __init__(self, name):
                self.name = name
                ppg.util.assert_uniqueness_of_object(self, also_check=[Dummy])

        Dummy("shu")

        with pytest.raises(ValueError):
            Dummy2("shu")

    def test_exception_on_run_without_previous_new_pipegraph(self):
        ppg.global_pipegraph = None
        with pytest.raises(ValueError):
            ppg.run()

    def test_flatten_jobs(self):
        ppg.new(run_mode=ppg.RunMode.NOTEBOOK)
        j1 = ppg.FileGeneratingJob("A", lambda of: "A")
        j2 = ppg.FileGeneratingJob("B", lambda of: "B")
        j3 = ppg.FileGeneratingJob("B", lambda of: "C")
        res = [j1, [j2, [j3, j1]]]
        # no dedup on this.
        assert list(ppg.util.flatten_jobs(res)) == [j1, j2, j3, j1]

    def test_inside_ppg(self):
        assert ppg.global_pipegraph is not None
        assert ppg.inside_ppg()
        ppg.global_pipegraph = None
        assert not ppg.inside_ppg()

    def test_wrap_for_function_invariant(self):
        from pathlib import Path

        ppg.new()

        def inner(x):
            return 5 + x

        func = ppg.util.wrap_for_function_invariant(inner, 1)
        j1 = ppg.FunctionInvariant("A", func)
        j2 = ppg.FileGeneratingJob(
            "BB", lambda of, func=func: of.write_text(str(func()))
        )
        j2.depends_on(j1)
        ppg.run()

        assert Path("BB").read_text() == "6"

        def inner(x):
            return 6 + x

        ppg.new(log_level=6)
        func1 = func
        func = ppg.util.wrap_for_function_invariant(inner, 1)
        assert func1 is not func
        assert func1.wrapped_function != func.wrapped_function
        j1 = ppg.FunctionInvariant("A", func)
        j2 = ppg.FileGeneratingJob(
            "BB", lambda of, func=func: of.write_text(str(func()))
        )
        j2.depends_on(j1)
        ppg.run()
        assert Path("BB").read_text() == "7"
