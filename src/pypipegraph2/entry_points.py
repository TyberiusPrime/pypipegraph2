import networkx
import sys
import subprocess


def browse_graph_ml(filename=None):
    graph = networkx.readwrite.graphml.read_graphml(filename)

    header = ("choose a node by typing. Ctrl-c to quit")
    p = subprocess.Popen(['fzf', '--header', header], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    chosen = p.communicate("\n".join(graph.nodes).encode('utf-8'))[0].decode('utf-8')
    stack = [chosen]

    if chosen:
        while True:
            if chosen.startswith('parent: ') or chosen.startswith('child: '):
                chosen = chosen[chosen.find(" ")+1:]
            chosen = chosen.strip()
            lines = []
            for child in reversed(sorted(graph.successors(chosen))):
                lines.append(f"child: {child}")
            lines.append("")
            for parent in reversed(sorted(graph.predecessors(chosen))):
                lines.append(f"parent: {parent}")
            lines = "\n".join(lines)
            header=f"Examining: {chosen}"
            p = subprocess.Popen(['fzf', '--header', header], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            chosen = p.communicate(lines.encode('utf-8'))[0].decode('utf-8')
            if not chosen:
                if len(stack) > 1:
                    chosen = stack.pop()
                    chosen = stack.pop()
                else:
                    break
            else:
                if not stack or stack[-1] != chosen:
                    stack.append(chosen)

def cli_browse_graph_ml():
    try:
        filename = sys.argv[1]
    except IndexError:
        print("ppg2_browse_graph_ml <path_to_.graphml>")
        sys.exit(1)
    browse_graph_ml(filename)
