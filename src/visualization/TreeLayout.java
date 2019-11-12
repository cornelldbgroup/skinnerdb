package visualization;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.PipeBase;
import org.graphstream.ui.geom.Point3;
import org.graphstream.ui.layout.Layout;

public class TreeLayout extends PipeBase implements Layout {
    final Graph internalGraph;
    Point3 hi, lo;
    long lastStep;
    int nodeMoved;
    boolean structureChanged;

    public TreeLayout() {
        internalGraph = new SingleGraph("hierarchical_layout-intern");
        hi = new Point3();
        lo = new Point3();
    }

    public void compute() {
        nodeMoved = 0;

        if (structureChanged) {
            structureChanged = false;
            computePositions();
            computePositionMetadata();
        }

        publishPositions();
        lastStep = System.currentTimeMillis();
    }

    private void computePositionMetadata() {
        hi.x = hi.y = Double.MIN_VALUE;
        lo.x = lo.y = Double.MAX_VALUE;

        for (int idx = 0; idx < internalGraph.getNodeCount(); idx++) {
            Node n = internalGraph.getNode(idx);
            double y = n.getNumber("y");
            double x = n.getNumber("x");

            if (!n.hasNumber("oldX") || n.getNumber("oldX") != x
                    || !n.hasNumber("oldY") || n.getNumber("oldY") != y) {
                n.setAttribute("oldX", x);
                n.setAttribute("oldY", y);
                n.addAttribute("changed");
                nodeMoved++;
            }

            hi.x = Math.max(hi.x, x);
            hi.y = Math.max(hi.y, y);
            lo.x = Math.min(lo.x, x);
            lo.y = Math.min(lo.y, y);
        }
    }

    private void computePositions() {

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

    public Point3 getHiPoint() {
        return hi;
    }

    public long getLastStepTime() {
        return lastStep;
    }

    public String getLayoutAlgorithmName() {
        return "Tree";
    }

    public Point3 getLowPoint() {
        return lo;
    }

    public int getNodeMovedCount() {
        return nodeMoved;
    }

    public double getQuality() {
        return 0;
    }

    @Override
    public double getForce() {
        return 0;
    }

    @Override
    public void clear() {}

    public double getStabilization() {
        return 1 - nodeMoved / (double) internalGraph.getNodeCount();
    }

    public double getStabilizationLimit() {
        return 1;
    }


    public int getSteps() {
        return 0;
    }

    public void moveNode(String id, double x, double y, double z) {}

    @Override
    public void freezeNode(String s, boolean b) {}

    public void setForce(double value) {}

    public void setQuality(double qualityLevel) {}

    public void setSendNodeInfos(boolean send) {}

    public void setStabilizationLimit(double value) {}

    public void shake() {}

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
