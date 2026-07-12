import pytest

from ppg3 import recipe
from ppg3.recipe import RecipeError

from conftest import requires_blake3


def make_fn_a():
    def f(x, y=1):
        return x + y

    return f


def make_fn_a_moved():
    # Same body, different line number in the file (achieved by padding
    # above it) — the hash must not change.

    def f(x, y=1):
        return x + y

    return f


def make_fn_renamed():
    def totally_different_name(x, y=1):
        return x + y

    return totally_different_name


def make_fn_different_body():
    def f(x, y=1):
        return x - y

    return f


def make_fn_different_default():
    def f(x, y=2):
        return x + y

    return f


@requires_blake3
def test_same_body_same_line_offset_same_hash():
    a = make_fn_a()
    b = make_fn_a_moved()
    assert recipe.recipe_hash(a) == recipe.recipe_hash(b)


@requires_blake3
def test_rename_does_not_change_hash():
    a = make_fn_a()
    renamed = make_fn_renamed()
    assert recipe.recipe_hash(a) == recipe.recipe_hash(renamed)


@requires_blake3
def test_different_body_changes_hash():
    a = make_fn_a()
    diff = make_fn_different_body()
    assert recipe.recipe_hash(a) != recipe.recipe_hash(diff)


@requires_blake3
def test_different_default_changes_hash():
    a = make_fn_a()
    diff = make_fn_different_default()
    assert recipe.recipe_hash(a) != recipe.recipe_hash(diff)


@requires_blake3
def test_decorator_stripped_from_hash():
    def deco(fn):
        return fn

    @deco
    def f(x):
        return x

    def g(x):
        return x

    assert recipe.recipe_hash(f) == recipe.recipe_hash(g)


def test_extract_defaults():
    def f(a, b=1, c="x"):
        pass

    defaults = recipe.extract_defaults(f)
    assert defaults == {"b": 1, "c": "x"}


def test_extract_source_and_name_keeps_real_name():
    def my_func(x):
        return x

    source, name = recipe.extract_source_and_name(my_func)
    assert name == "my_func"
    assert "def my_func(" in source


def test_extract_normalized_source_blanks_name():
    def my_func(x):
        return x

    normalized = recipe.extract_normalized_source(my_func)
    assert "my_func" not in normalized
    assert "def <func>(" in normalized


def test_recipe_hash_builtin_raises():
    with pytest.raises(RecipeError):
        recipe.recipe_hash(len)


@requires_blake3
def test_recipe_hash_command_stable_and_sensitive():
    argv1 = ["{tool:samtools}", "view", "{in:bam}", "{out}"]
    argv2 = ["{tool:samtools}", "view", "{in:bam}", "{out}"]
    argv3 = ["{tool:samtools}", "index", "{in:bam}", "{out}"]
    assert recipe.recipe_hash_command(argv1) == recipe.recipe_hash_command(argv2)
    assert recipe.recipe_hash_command(argv1) != recipe.recipe_hash_command(argv3)


def test_recipe_hash_command_rejects_non_strings():
    with pytest.raises(RecipeError):
        recipe.recipe_hash_command(["ok", 42])


@requires_blake3
def test_recipe_hash_source_sensitive_to_content_and_qualname():
    h1 = recipe.recipe_hash_source(b"print(1)", "foo")
    h2 = recipe.recipe_hash_source(b"print(1)", "bar")
    h3 = recipe.recipe_hash_source(b"print(2)", "foo")
    assert h1 != h2
    assert h1 != h3


@requires_blake3
def test_recipe_hash_source_includes_participate():
    base = recipe.recipe_hash_source(b"print(1)", "foo", [b"a"])
    other = recipe.recipe_hash_source(b"print(1)", "foo", [b"b"])
    assert base != other
