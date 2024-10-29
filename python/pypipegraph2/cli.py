"""CLI interface to ppg2"""

import pyzstd
import re
import sys
import pypipegraph2 as ppg2


def main_filter_constraint_violations(filename=None, pipegraph=None):
    """For when you really need to remove some jobs from the pipegraph's history.
    Takes a filename with job ids.
    Defaults to .ppg/errors/latest/constraint_violations.jobs

    If you have your ppg history in a non standard place, pass in a ppg
    with the correct directories set.
    """
    if "--help" in sys.argv:
        print(
            "ppg2-cli filter_constraint_violations [filename] - filter constraint violations from history"
        )
    if pipegraph is None:
        pipegraph = ppg2.new()

    if sys.argv[1:]:
        filename = Path(sys.argv[1])
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
                self.descendants = sorted(
                    self.graph.successors(self.selected_node)
                )
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

def hash_file(filename= None):
    from pathlib import Path
    import hashers
    if filename is None:
        filename = sys.argv[1]
    filename = Path(filename)
    h = hashers.hash_file(filename)
    import pprint
    pprint.pprint(h)



if __name__ == "__main__":
    import sys

    view_graph_ml(filename=sys.argv[1])
