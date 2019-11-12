package visualization;

import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.PipeBase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TreeLayout extends PipeBase {
    private class BuccheimState {
        // Graph Metadata
        List<List<Integer>> children;
        int[] leftSibling, leftMostSibling, parent;

        // Buccheim Data Structures
        int[] thread, ancestor, number;
        double[] mod, prelim, change, shift;

        public BuccheimState(Graph graph, int root) {
            int numNodes = graph.getNodeCount();
            leftSibling = new int[numNodes];
            leftMostSibling = new int[numNodes];
            parent = new int[numNodes];
            number = new int[numNodes];
            children = new ArrayList<>();
            for (int i = 0; i < numNodes; i++) {
                children.add(new ArrayList<>());
            }

            computeGraphMetadata(root);

            thread = new int[numNodes];
            ancestor = new int[numNodes];
            mod = new double[numNodes];
            prelim = new double[numNodes];
            change = new double[numNodes];
            shift = new double[numNodes];

            for (int i = 0; i < numNodes; i++) {
                thread[i] = -1;
                ancestor[i] = i;
                mod[i] = 0;
                change[i] = 0;
                shift[i] = 0;
            }
        }

        private void computeGraphMetadata(int n) {
            Node node = internalGraph.getNode(n);
            List<Edge> leavingEdges = new ArrayList<>(node.getLeavingEdgeSet());
            leavingEdges.sort(Comparator.comparing(e -> e.getTargetNode().getId()));

            int previous = -1;
            int farLeftSibling = -1;

            int i = 0;
            for (Edge e : leavingEdges) {
                int child = e.getTargetNode().getIndex();
                number[child] = i;
                parent[child] = n;
                children.get(n).add(child);
                leftSibling[child] = previous;

                if (farLeftSibling < 0) {
                    farLeftSibling = child;
                } else {
                    leftMostSibling[child] = farLeftSibling;
                }

                computeGraphMetadata(child);

                previous = child;
                i++;
            }
        }
    }


    final Graph internalGraph;
    private boolean structureChanged;
    private final int DISTANCE = 1;
    private final double LEVEL_SPACE = 1;
    private BuccheimState state;


    public TreeLayout() {
        internalGraph = new SingleGraph("treelayout-internal");
        state = null;
        structureChanged = false;
    }

    public void compute() {
        if (structureChanged) {
            structureChanged = false;
            computePositions();
            computePositionMetadata();
        }

        publishPositions();
    }

    private void computePositionMetadata() {
        for (int idx = 0; idx < internalGraph.getNodeCount(); idx++) {
            Node n = internalGraph.getNode(idx);
            double y = n.getNumber("y");
            double x = n.getNumber("x");

            if (!n.hasNumber("oldX") || n.getNumber("oldX") != x
                    || !n.hasNumber("oldY") || n.getNumber("oldY") != y) {
                n.setAttribute("oldX", x);
                n.setAttribute("oldY", y);
                n.addAttribute("changed");
            }
        }
    }

    // Implementation of Buccheim's Tree Layout Algorithm
    // http://dirk.jivas.de/papers/buchheim02improving.pdf
    private void computePositions() {
        Node rootNode = internalGraph.getNode("root");
        int r = rootNode.getIndex();
        state = new BuccheimState(internalGraph, r);
        firstWalk(r);
        secondWalk(r, -state.prelim[r], 0);
    }

    private void secondWalk(int v, double m, int depth) {
        Node node = internalGraph.getNode(v);
        node.setAttribute("x", state.prelim[v] + m);
        node.setAttribute("y", -LEVEL_SPACE * depth);

        for (int w : state.children.get(v)) {
            secondWalk(w, m + state.mod[v], depth + 1);
        }
    }

    private void firstWalk(int v) {
        if (state.children.get(v).size() == 0) {
            int w = state.leftSibling[v];
            if (w >= 0) {
                state.prelim[v] = state.prelim[w] + DISTANCE;
            } else {
                state.prelim[v] = 0;
            }
        } else {
            List<Integer> children = state.children.get(v);
            int leftMostChild = children.get(0);
            int rightMostChild = children.get(children.size() - 1);
            int defaultAncestor = leftMostChild;

            for (int w : children) {
                firstWalk(w);
                defaultAncestor = apportion(w, defaultAncestor);
            }

            executeShifts(v);
            double midpoint = 0.5 *
                    (state.prelim[leftMostChild] +
                            state.prelim[rightMostChild]);

            int left = state.leftSibling[v];
            if (left >= 0) {
                state.prelim[v] = state.prelim[left] + DISTANCE;
                state.mod[v] = state.prelim[v] - midpoint;
            } else {
                state.prelim[v] = midpoint;
            }
        }
    }

    private int apportion(int v, int defaultAncestor) {
        int w = state.leftSibling[v];
        if (w >= 0) {
            int vip = v;
            int vop = v;
            int vim = w;
            int vom = state.leftMostSibling[vip];

            double sip = state.mod[vip];
            double sop = state.mod[vop];
            double sim = state.mod[vim];
            double som = state.mod[vom];

            while (true) {
                int nr = nextRight(vim);
                int nl = nextLeft(vip);

                if (nr < 0 || nl < 0) {
                    break;
                }

                vim = nr;
                vip = nl;
                vom = nextLeft(vom);
                vop = nextRight(vop);
                state.ancestor[vop] = v;
                double shift =
                        (state.prelim[vim] + sim) -
                                (state.prelim[vip] + sip) + DISTANCE;
                if (shift > 0) {
                    moveSubtree(ancestor(vim, v, defaultAncestor), v, shift);
                    sip = sip + shift;
                    sop = sop + shift;
                }

                sim = sim + state.mod[vim];
                sip = sip + state.mod[vip];
                som = som + state.mod[vom];
                sop = sop + state.mod[vop];
            }

            int nr = nextRight(vim);
            int nl = nextLeft(vim);
            if (nr >= 0 && nextRight(vop) < 0) {
                state.thread[vop] = nr;
                state.mod[vop] = state.mod[vop] + sim - sop;
            }
            if (nl >= 0 && nextLeft(vom) < 0) {
                state.thread[vom] = nl;
                state.mod[vom] = state.mod[vom] + sim - som;
                defaultAncestor = v;
            }
        }
        return defaultAncestor;
    }

    private int nextLeft(int v) {
        if (state.children.get(v).size() > 0) {
            return state.children.get(v).get(0);
        }

        return state.thread[v];
    }

    private int nextRight(int v) {
        if (state.children.get(v).size() > 0) {
            return state.children.get(v).get(state.children.get(v).size() - 1);
        }

        return state.thread[v];
    }

    private int ancestor(int vim, int v, int defaultAncestor) {
        if (state.parent[state.ancestor[vim]] == state.parent[v]) {
            return state.ancestor[vim];
        }
        return defaultAncestor;
    }

    private void moveSubtree(int wm, int wp, double shift) {
        double subtrees = state.number[wp] - state.number[wm];
        state.change[wp] -= shift / subtrees;
        state.shift[wp] += shift;
        state.change[wm] += shift / subtrees;
        state.prelim[wp] += shift;
        state.mod[wp] += shift;
    }

    private void executeShifts(int v) {
        double shift = 0;
        double change = 0;

        List<Integer> children = state.children.get(v);

        for (int i = children.size() - 1; i >= 0; i--) {
            int w = children.get(i);
            state.prelim[w] += shift;
            state.mod[w] += shift;
            change += state.change[w];
            shift += state.shift[w] + change;
        }
    }

    private void publishPositions() {
        for (Node n : internalGraph) {
            if (n.hasAttribute("changed")) {
                n.removeAttribute("changed");

                sendNodeAttributeChanged(sourceId, n.getId(), "xyz", null,
                        new double[]{n.getNumber("x"), n.getNumber("y"), 0});
            }
        }
    }

    public void nodeAdded(String sourceId, long timeId, String nodeId) {
        internalGraph.addNode(nodeId);
        structureChanged = true;
    }

    public void nodeRemoved(String sourceId, long timeId, String nodeId) {
        internalGraph.removeNode(nodeId);
        structureChanged = true;
    }

    public void edgeAdded(String sourceId, long timeId, String edgeId,
                          String fromId, String toId, boolean directed) {
        internalGraph.addEdge(edgeId, fromId, toId, directed);
        structureChanged = true;
    }

    public void edgeRemoved(String sourceId, long timeId, String edgeId) {
        internalGraph.removeEdge(edgeId);
        structureChanged = true;
    }

    public void graphCleared(String sourceId, long timeId) {
        internalGraph.clear();
        structureChanged = true;
    }
}
