"""CLI interface to ppg2"""

import subprocess
import pyzstd
import re
import sys
import pypipegraph2 as ppg2
from pathlib import Path



def main_filter_constraint_violations(dir_config=None, filename=None, pipegraph=None):
    """For when you really need to remove some jobs from the pipegraph's history.
    Takes a filename with job ids.
    Defaults to .ppg/errors/latest/constraint_violations.jobs

    If you have your ppg history in a non standard place, pass in a ppg
    with the correct directories set.
    """
    if "--help" in sys.argv:
        print(
            "ppg2-cli filter_constraint_violations [dir_config] <filename> - filter constraint violations from history. Dir_config typicall is .ppg/per_script/<your_script>"
        )
    if pipegraph is None:
        pipegraph = ppg2.new()

    if dir_config is None and sys.argv[1:]:
        dir_config = Path(sys.argv[1])
    if filename is None and sys.argv[2:]:
        filename = Path(sys.argv[2])
    if pipegraph is None:
        pipegraph = ppg2.new(dir_config=ppg.DirConfig(dir_config))
    if filename is None:
        filename = pipegraph.dir_config.error_dir / "latest/constraint_violations.jobs"

    q = filename.read_text().strip().split("\n")
    print("reading history")
    history = pipegraph._load_history()

    to_del = set()
    for key, v in history.items():
        found = False
        for x in q:
            if key.startswith(x) or key.endswith(x):
                print("Would delete")
                print("\thistory: ", key)
                print("\tbecause: ", x)
                print("")
                to_del.add(key)
                found = True
            if found:
                break
    print(f"Delete {len(to_del)} history entries?")
    print("type yes<enter> to proceed")
    print("This tool does not create backups. Make a snapshot of your project first")
    for x in to_del:
        del history[x]
    if input() == "yes":
        ppg2.global_pipegraph._save_history(history)
    else:
        print("aborted")


def view_graph_ml(filename=None):
    if filename is None:
        try:
            filename = Path(sys.argv[1])
        except:
            print("usage: ppg2-browse-graph <graphml file>")
            print(
                "create a graphml in .ppg/../logs by calling ppg2.run with dump_graphml=True"
            )
            sys.exit(1)
    from rich.console import Console
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.text import Text
    from rich.live import Live
    from rich.table import Table
    from rich.prompt import Prompt
    from rich.align import Align
    import networkx as nx

    graph = nx.read_graphml(filename)
    print(len(graph.nodes), len(graph.edges))

    from textual.app import App, ComposeResult
    from textual.containers import Container
    from textual.widgets import Input, ListView, Static, ListItem, Label
    from textual.reactive import reactive
    import networkx as nx

    class GraphExplorerApp(App):
        # CSS_PATH = "styles.css"  # Optional: Define custom CSS here for styling
        ancestors = reactive([])
        descendants = reactive([])

        def __init__(self, graph):
            super().__init__()
            self.graph = graph
            self.nodes = sorted([str(x) for x in self.graph.nodes])
            self.filtered_nodes = reactive(sorted(self.graph.nodes()))
            self.selected_node = reactive(None)

        def compose(self) -> ComposeResult:
            # Build the UI structure
            yield Container(
                Container(
                    Static("Upstream", id="ancestors-label"),
                    ListView(id="ancestors"),
                    id="left-pane",
                ),
                Container(
                    Input(placeholder="Search nodes...", id="node-input"),
                    Static("Filtered Nodes", id="nodes-label"),
                    ListView(id="node-list"),
                    id="middle-pane",
                ),
                Container(
                    Static("Downstream", id="descendants-label"),
                    ListView(id="descendants"),
                    id="right-pane",
                ),
                id="main-layout",
            )

        def on_mount(self):
            self.query_one("#node-input").focus()  # Start focus on text input

        def on_input_changed(self, event: Input.Changed):
            # Filter nodes based on input text
            search_text = event.value.strip().lower()
            self.filtered_nodes = [
                node
                for node in self.nodes
                if not search_text or search_text in str(node).lower()
            ]
            self.update_filtered_nodes()
            self.seected_node = None
            self.update_ancestors_descendants()

        def update_filtered_nodes(self):
            node_list_view = self.query_one("#node-list", ListView)
            node_list_view.clear()
            for node in self.filtered_nodes[:100]:
                node_list_view.append(ListItem(Label(node), name=node))

        def update_ancestors_descendants(self):
            if self.selected_node:
                self.ancestors = sorted(self.graph.predecessors(self.selected_node))
                self.descendants = sorted(self.graph.successors(self.selected_node))
                ancestors_label = self.query_one("#ancestors-label", Static)
                ancestors_label.update(f"Upstream ({len(self.ancestors)})")
                # Populate ancestor list
                ancestors_view = self.query_one("#ancestors", ListView)
                ancestors_view.clear()
                for ancestor in self.ancestors:
                    ancestors_view.append(ListItem(Label(ancestor)))

                # Populate descendant list
                descendants_view = self.query_one("#descendants", ListView)
                descendants_view.clear()
                for descendant in self.descendants:
                    descendants_view.append(ListItem(Label(descendant)))

        def on_list_view_highlighted(self, event: ListView.Selected):
            # Update selected node when a node is chosen from the list
            if event.item:
                selected_node = event.item.name
            else:
                selected_node = None
            self.selected_node = selected_node
            self.update_ancestors_descendants()

        def on_key(self, event):
            # Tab to switch focus between input, node list, ancestors, and descendants
            if event.key == "tab":
                if self.query_one("#node-input").has_focus:
                    self.query_one("#node-list").focus()
                elif self.query_one("#node-list").has_focus:
                    self.query_one("#ancestors").focus()
                elif self.query_one("#ancestors").has_focus:
                    self.query_one("#descendants").focus()
                else:
                    self.query_one("#node-input").focus()

            # Enter to select item or switch focus back to the middle column
            elif event.key == "enter":
                if (
                    self.query_one("#ancestors").has_focus
                    or self.query_one("#descendants").has_focus
                ):
                    self.query_one("#node-list").focus()
            elif event.key == "escape":
                sys.exit(0)

        # Run the app

    GraphExplorerApp(graph).run()


def hash_file(filename=None):
    from pathlib import Path
    import hashers

    if filename is None:
        filename = sys.argv[1]
    filename = Path(filename)
    h = hashers.hash_file(filename)
    import pprint

    pprint.pprint(h)


def browse_history(dot_ppg_path=None, dot_ppg_path_old=None):
    from pathlib import Path

    if dot_ppg_path is None:
        try:
            dot_ppg_path = Path(sys.argv[1])
        except:
            print(
                "usage: ppg2-browse-history <.ppp/per_script/whatever> [<old/.ppp/per_script/whatever] [search_text]"
            )
            print(
                "second one is for diffs. Might alse be a 'snapshot' directory, then path1 is appended for maximum convience."
            )
            sys.exit(1)
    filename = Path(dot_ppg_path) / "history/ppg_history.2.zstd"
    if dot_ppg_path_old is None:
        try:
            dot_ppg_path_old = Path(sys.argv[2])
        except IndexError:
            pass
    import networkx as nx
    import pyzstd
    import tempfile
    import subprocess
    import json

    with pyzstd.ZstdFile(filename, "rb") as op:
        history = json.loads(op.read().decode("utf-8"))
    history_old = None
    if dot_ppg_path_old is not None:
        filename_old = Path(dot_ppg_path_old) / "history/ppg_history.2.zstd"
        if filename_old.exists():
            with pyzstd.ZstdFile(filename_old, "rb") as op:
                history_old = json.loads(op.read().decode("utf-8"))
        else:
            filename_old = (
                Path(dot_ppg_path_old) / dot_ppg_path / "history/ppg_history.2.zstd"
            )
            with pyzstd.ZstdFile(filename_old, "rb") as op:
                history_old = json.loads(op.read().decode("utf-8"))
    try:
        start_search_term = sys.argv[3]
    except:
        start_search_term = ""

    from textual.app import App, ComposeResult
    from textual.containers import Container
    from textual.widgets import (
        Input,
        ListView,
        Static,
        ListItem,
        Label,
        TextArea,
        TabbedContent,
    )
    from textual.reactive import reactive
    import difflib
    import networkx as nx

    class HistoryExplorerApp(App):
        # CSS_PATH = "styles.css"  # Optional: Define custom CSS here for styling
        ancestors = reactive([])
        descendants = reactive([])

        def __init__(self, history, history_old, start_search_term):
            super().__init__()
            self.history = history
            self.history_old = history_old
            self.start_search = start_search_term
            self.filtered_nodes = []

        def compose(self) -> ComposeResult:
            # Build the UI structure
            with Container(id="main-layout"):
                yield Container(
                    Input(placeholder="Search history...", id="node-input"),
                    Static("Filtered history", id="nodes-label"),
                    ListView(id="node-list"),
                    Static("Selected node", id="nodes-selected-label"),
                    TextArea("Selected Node", id="node-selected"),
                    id="middle-pane",
                )
                if self.history_old:
                    with Container(id="right-pane"):
                        with TabbedContent("Content", "Content old", "Diff"):
                            yield TextArea(id="node_history")
                            yield TextArea(id="node_history_old")
                            yield TextArea(id="node_history_diff")
                else:
                    yield Container(
                        Static(
                            "Content | Content Old | Diff"
                            if self.history_old
                            else "Content",
                            id="descendants-label",
                        ),
                        Label(id="node_history"),
                        id="right-pane",
                    )

        def on_mount(self):
            ni = self.query_one("#node-input")
            ni.focus()  # Start focus on text input
            ni.value = self.start_search

        def on_input_changed(self, event: Input.Changed):
            # Filter nodes based on input text
            search_text = event.value.strip().lower()
            # so we can copy paste into this.
            search_text = search_text.replace("'", "")
            search_text = search_text.replace("->", "!!!")
            self.filtered_nodes = [
                node
                for node in self.history
                if not search_text or search_text in str(node).lower()
            ]
            self.update_filtered_nodes()
            self.selected_node = None
            self.update_node()

        def update_filtered_nodes(self):
            node_list_view = self.query_one("#node-list", ListView)
            node_list_view.clear()
            for node in self.filtered_nodes[:100]:
                node_list_view.append(ListItem(Label(node), name=node))

        def update_node(self):
            label_node = self.query_one("#node-selected", TextArea)
            label_node.text = (
                (self.selected_node or "")
                .replace(":::", ":::\n")
                .replace("!!!", "\n!!!\n")
                .strip()
            )
            text_box = self.query_one("#node_history", TextArea)
            if self.selected_node:
                text = self.history[self.selected_node]
            else:
                text = "(no node selected)"
            text_box.text = text

            if self.history_old:
                if self.selected_node:
                    old_text = self.history_old.get(
                        self.selected_node, "(no history available)"
                    )
                else:
                    old_text = "(no node selected)"
                text_box = self.query_one("#node_history_old", TextArea)
                text_box.text = old_text
                if old_text == text:
                    diff_text = "No changes"
                else:
                    diff_text = ""
                    tf = open("a.txt",'w')
                    tf2 = open('b.txt', 'w')
                    tf.write(old_text)
                    tf2.write(text)
                    tf.flush()
                    tf2.flush()
                    try:
                        diff_text = subprocess.check_output(['icdiff', tf.name, tf2.name]).decode('utf-8')
                    except FileNotFoundError:
                        diff_text = "(install icdiff for pretty diff)\n"

                        for line in difflib.unified_diff(
                            old_text.splitlines(), text.splitlines()
                        ):
                            diff_text += line + "\n"
                text_box = self.query_one("#node_history_diff", TextArea)
                text_box.text = diff_text

        def on_list_view_highlighted(self, event: ListView.Selected):
            # Update selected node when a node is chosen from the list
            if event.item:
                selected_node = event.item.name
            else:
                selected_node = None
            self.selected_node = selected_node
            self.update_node()

        def on_key(self, event):
            # Tab to switch focus between input, node list, ancestors, and descendants
            if event.key == "tab":
                if self.query_one("#node-input").has_focus:
                    self.query_one("#node-list").focus()
                elif self.query_one("#node-list").has_focus:
                    self.query_one("#node_history").focus()
                elif self.query_one("#node_history").has_focus:
                    if self.history_old:
                        self.query_one("#node_history_old").focus()
                    else:
                        self.query_one("#node-list").focus()
                elif self.query_one("#node_history_old").has_focus:
                    self.query_one("#node_history_diff").focus()
                else:
                    self.query_one("#node-input").focus()
            elif event.key == "down" and self.query_one("#node-input").has_focus:
                self.query_one("#node-list").focus()

            elif event.key == "escape" or event.key == 'q':
                sys.exit(0)

        # Run the app

    HistoryExplorerApp(history, history_old, start_search_term).run()


if __name__ == "__main__":
    import sys

    browse_history()
