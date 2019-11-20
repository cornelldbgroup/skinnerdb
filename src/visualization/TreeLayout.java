package visualization;

import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Implementation of Buccheim's Tree Layout Algorithm
 * Paper: http://dirk.jivas.de/papers/buchheim02improving.pdf
 */
public class TreeLayout {
    /**
     * State variables for the algorithm
     */
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

        /**
         * Populates parent/children/sibling maps for the subtree with root n
         *
         * @param n root of the subtree
         */
        private void computeGraphMetadata(int n) {
            Node node = internalGraph.getNode(n);
            List<Edge> leavingEdges = new ArrayList<>(node.getLeavingEdgeSet());
            leavingEdges.sort(Comparator.comparing(e ->
                    e.getTargetNode().getId()));

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

    final Graph internalGraph, outputGraph;
    private boolean structureChanged;
    private final double DISTANCE = 1.25;
    private final double LEVEL_SPACE = 1;
    private BuccheimState state;

    /**
     * @param outputGraph display graph
     */
    public TreeLayout(Graph outputGraph) {
        internalGraph = new SingleGraph("treelayout-internal");
        state = null;
        structureChanged = false;
        this.outputGraph = outputGraph;
    }

    /**
     * Compute layouts and publish changes to the display graph if needed
     */
    public void compute() {
        if (structureChanged) {
            structureChanged = false;
            computePositions();
            computePositionMetadata();
        }

        publishPositions();
    }

    /**
     * Compute whether or not nodes have changed since the last position
     * calculation.
     */
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

    /**
     * The main method of the Buccheim algorithm.
     */
    private void computePositions() {
        Node rootNode = internalGraph.getNode("root");
        int r = rootNode.getIndex();
        state = new BuccheimState(internalGraph, r);
        firstWalk(r);
        secondWalk(r, -state.prelim[r], 0);
    }

    /**
     * DFS through the tree and set the x position to be the prelim with any
     * modifiers
     *
     * @param v     current node
     * @param m     modifier value
     * @param depth depth in tree
     */
    private void secondWalk(int v, double m, int depth) {
        Node node = internalGraph.getNode(v);
        node.setAttribute("x", state.prelim[v] + m);
        node.setAttribute("y", -LEVEL_SPACE * depth);

        for (int w : state.children.get(v)) {
            secondWalk(w, m + state.mod[v], depth + 1);
        }
    }

    /**
     * Computes a preliminary x coordinate for a node
     *
     * @param v the node
     */
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

    /**
     * Compute any modifiers needed to prevent subtrees from overallping.
     *
     * @param v               the node
     * @param defaultAncestor the left most child of v's parent
     * @return
     */
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

    /**
     * Get the next node on the left most contour of the subtree.
     *
     * @param v the current node.
     * @return the successor on the countour
     */
    private int nextLeft(int v) {
        if (state.children.get(v).size() > 0) {
            return state.children.get(v).get(0);
        }

        return state.thread[v];
    }

    /**
     * Get the next node on the right most contour of this subtree.
     *
     * @param v the current node.
     * @return the successor on the countour
     */
    private int nextRight(int v) {
        if (state.children.get(v).size() > 0) {
            return state.children.get(v).get(state.children.get(v).size() - 1);
        }

        return state.thread[v];
    }

    /**
     * Leftmost greatest uncommon ancestor of vim and its right neighbor
     *
     * @param vim
     * @param v
     * @param defaultAncestor
     * @return the ancestor
     */
    private int ancestor(int vim, int v, int defaultAncestor) {
        if (state.parent[state.ancestor[vim]] == state.parent[v]) {
            return state.ancestor[vim];
        }
        return defaultAncestor;
    }

    /**
     * Shift wp's subtree by shift. Prepare for the shifts for the subtrees
     * between wp and wm.
     *
     * @param wm
     * @param wp
     * @param shift
     */
    private void moveSubtree(int wm, int wp, double shift) {
        double subtrees = state.number[wp] - state.number[wm];
        state.change[wp] -= shift / subtrees;
        state.shift[wp] += shift;
        state.change[wm] += shift / subtrees;
        state.prelim[wp] += shift;
        state.mod[wp] += shift;
    }

    /**
     * Use the result of moveSubtree to shift the subtrees.
     *
     * @param v the subtree rooted at v
     */
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

    /**
     * Send back positions to the display graph from the internal
     * representation.
     */
    private void publishPositions() {
        for (Node n : internalGraph) {
            if (n.hasAttribute("changed")) {
                n.removeAttribute("changed");

                outputGraph.getNode(n.getId()).setAttribute("xyz",
                        n.getNumber("x"), n.getNumber("y"));
            }
        }
    }

    /**
     * Add a new node to the internal representation.
     *
     * @param nodeId the ID of the node
     */
    public void nodeAdded(String nodeId) {
        internalGraph.addNode(nodeId);
        structureChanged = true;
    }

    /**
     * Add a new edge to the internal representation.
     *
     * @param edgeId   the ID of the edge
     * @param fromId   ID fo the source node
     * @param toId     ID of the target node
     * @param directed whether it is directed
     */
    public void edgeAdded(String edgeId, String fromId, String toId,
                          boolean directed) {
        internalGraph.addEdge(edgeId, fromId, toId, directed);
        structureChanged = true;
    }
}
