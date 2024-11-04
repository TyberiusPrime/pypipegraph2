"""Borrowed and adapted from 'rich'

Copyright 2020 Will McGugan, Florian Finkernagel

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""

from types import TracebackType
from typing import Dict, List, Type
from traceback import walk_tb
import inspect
import sys
import os
from dataclasses import dataclass, field
import textwrap

_load_cwd = os.path.abspath(os.getcwd())


@dataclass
class Frame:
    filename: str
    lineno: int
    name: str
    locals: Dict[str, str]
    source: str


@dataclass
class Stack:
    exc_type: str
    exc_value: str
    is_cause: bool = False
    frames: List[Frame] = field(default_factory=list)


class Trace:
    def __init__(
        self,
        exc_type: Type[BaseException],
        exc_value: BaseException,
        traceback: TracebackType,
    ):
        """Extract traceback information.

        Args:
            exc_type (Type[BaseException]): Exception type.
            exc_value (BaseException): Exception value.
            traceback (TracebackType): Python Traceback object.

        Returns:
            Trace: A Trace instance which you can use to construct a `Traceback`.
        """

        stacks: List[Stack] = []
        is_cause = False

        while True:
            stack = Stack(
                exc_type=str(exc_type.__name__),
                exc_value=str(exc_value),
                is_cause=is_cause,
            )

            stacks.append(stack)
            append = stack.frames.append

            for frame_summary, line_no in walk_tb(traceback):
                try:
                    if (
                        inspect.getsourcefile(frame_summary) == sys.argv[0]
                    ):  # current script, not absolute
                        filename = os.path.join(
                            _load_cwd, sys.argv[0]
                        )  # pragma: no cover
                    else:
                        filename = inspect.getabsfile(frame_summary)
                except Exception:  # pragma: no cover
                    filename = frame_summary.f_code.co_filename
                    if filename and not filename.startswith("<"):
                        filename = os.path.abspath(filename) if filename else "?"
                try:
                    with open(filename, "rb") as op:
                        source = op.read().decode("utf-8", errors="replace")
                except Exception:  # pragma: no cover
                    source = ""
                # this needs to be 'robust'
                # exceptions here tend to not leave a decent stack trace
                my_locals = {}
                for key, value in frame_summary.f_locals.items():
                    try:
                        my_locals[key] = str(value)
                    except Exception as e:
                        my_locals[key] = f"Could not str() local: {e}"
                frame = Frame(
                    filename=filename,
                    lineno=line_no,
                    name=frame_summary.f_code.co_name,
                    locals=my_locals,
                    source=source,
                )
                append(frame)

            cause = getattr(exc_value, "__cause__", None)
            if cause and cause.__traceback__:
                exc_type = cause.__class__
                exc_value = cause
                traceback = cause.__traceback__
                if traceback:
                    is_cause = not getattr(exc_value, "__suppress_context__", False)
                    stack.is_cause = is_cause
                    continue

            # No cover, code is reached but coverage doesn't recognize it.
            break  # pragma: no cover

        self.stacks = stacks[::-1]

    def __str__(self):
        return self._format_rich_traceback_fallback(False)

    def _format_rich_traceback_fallback(
        self, include_locals=False, include_formating=True,
        skip_ppg = False,
    ):
        """Pretty print a traceback.

        We don't use rich's own facility, since it is
        not time-bounded /  does not cut the output
        """

        def bold(x):
            if include_formating:
                return f"[bold]{x}[/bold]"
            else:
                return x

        def red(x):
            if include_formating:
                return f"[red]{x}[/red]"
            else:
                return x

        if include_locals:

            def render_locals(frame):
                out.append(bold("Locals") + ":")
                scope = frame.locals
                items = sorted(scope.items())
                len_longest_key = max((len(x[0]) for x in items))
                for key, value in items:
                    v = str(value)
                    if len(v) > 1000:
                        v = v[:1000] + "…"
                    v = textwrap.indent(v, "\t   " + " " * len_longest_key).lstrip()
                    out.append(f"\t{key.rjust(len_longest_key, ' ')} = {v}")
                out.append("")

        else:

            def render_locals(frame):
                pass

        first_stack = True
        out = []
        if self is None:
            out = ["# no traceback was captured"]
        else:
            for stack in self.stacks:
                if not first_stack:
                    out.append("")
                    if stack.is_cause:
                        out.append("The above exception cause to the following one")
                first_stack = False
                exc_value = str(stack.exc_value)
                if len(exc_value) > 1000:
                    exc_value = exc_value[:1000] + "…"
                out.append(
                    f"{bold('Exception')}: {red(bold(stack.exc_type) + ' ' + exc_value)}"
                )
                out.append(f"{bold('Traceback')} (most recent call last):")

                for frame in stack.frames:
                    if skip_ppg and 'python/pypipegraph2' in frame.filename:
                        out.append(f'{frame.filename}":{frame.lineno}, in {frame.name} (details skipped)')
                        continue

                    out.append(f'{frame.filename}":{frame.lineno}, in {frame.name}')
                    # if frame.filename.startswith("<"): # pragma: no cover # - interactive, I suppose
                    # render_locals(frame)
                    # continue
                    extra_lines = 3
                    if frame.source:
                        code = frame.source
                        line_range = (
                            frame.lineno - extra_lines,
                            frame.lineno + extra_lines,
                        )
                        # leading empty lines get filtered from the output
                        # but to actually show the correct section & highlight
                        # we need to adjust the line range accordingly.
                        code = code.split("\n")
                        for ii, line in zip(
                            range(*line_range), code[line_range[0] : line_range[1]]
                        ):
                            if ii == frame.lineno - 1:
                                c = "> "
                            else:
                                c = "  "
                            out.append(f"\t{c}{ii} {line}")
                        if frame.locals:
                            render_locals(frame)
                        continue
                    else:
                        out.append("# no source available")
                        if frame.locals:
                            render_locals(frame)
                out.append(
                    f"{bold('Exception')} (repeated from above): {red(bold(stack.exc_type) + ' ' +  exc_value)}"
                )
        return "\n".join(out)
