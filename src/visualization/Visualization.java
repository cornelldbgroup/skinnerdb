package visualization;

import org.graphstream.graph.Edge;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.ui.graphicGraph.stylesheet.StyleConstants;
import org.graphstream.ui.layout.LayoutRunner;
import org.graphstream.ui.spriteManager.Sprite;
import org.graphstream.ui.spriteManager.SpriteManager;
import org.graphstream.ui.swingViewer.ViewPanel;
import org.graphstream.ui.view.Viewer;
import query.QueryInfo;

import javax.swing.*;
import java.awt.event.MouseListener;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Creates and manages a visualization of the query.
 */
public class Visualization extends BaseMouseListener {
    private SingleGraph graph;
    private Viewer viewer;
    private SpriteManager spriteManager;
    private TreeLayout layout;
    private QueryInfo info;
    private JLabel counterLabel;
    private int iterationCounter;
    private Map<String, Integer> numVisits;
    private Map<String, Double> maxReward;
    private Map<String, Double> rewardSum;

    private final String stylesheet = "" +
            "graph {" +
            " padding: 60px;" +
            "}" +
            "" +
            "sprite.counter {" +
            " fill-mode: none;" +
            " text-size: 12px;" +
            "} " +
            "" +
            "sprite.join { " +
            " shape: flow; " +
            " size: 5px;" +
            " z-index: 0; " +
            " sprite-orientation: from;" +
            " fill-color: green;" +
            "} " +
            "" +
            "node {" +
            " size: 35px;" +
            " fill-color: white;" +
            " text-color: white;" +
            " text-style: bold;" +
            " text-padding: 2px;" +
            " text-size: 13px;" +
            " text-background-mode: rounded-box;" +
            " text-background-color: rgb(35, 47, 62);" +
            "}" +
            "" +
            "edge {" +
            "  arrow-shape: none;" +
            "}";

    /**
     * Initializes the state/data structures of the query visualizer.
     *
     * @param info the query
     */
    public void init(QueryInfo info) {
        this.info = info;
        // Prevent the Graphstream Layout manager from logging to std out
        Logger.getLogger(LayoutRunner.class.getSimpleName())
                .setUseParentHandlers(false);

        // Initialize viewer
        System.setProperty("org.graphstream.ui.renderer",
                "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
        graph = new SingleGraph("Join Order");
        viewer = graph.display(false);
        viewer.setCloseFramePolicy(Viewer.CloseFramePolicy.CLOSE_VIEWER);
        viewer.disableAutoLayout();
        ViewPanel view = viewer.getDefaultView();
        for (MouseListener listener : view.getMouseListeners()) {
            view.removeMouseListener(listener);
        }
        view.addMouseListener(this);
        view.setLayout(null);

        // Setup layout
        layout = new TreeLayout(graph);

        iterationCounter = 0;
        counterLabel = new JLabel("Number of Iterations: " + iterationCounter);
        counterLabel.setBounds(25, 0, 200, 50);
        view.add(counterLabel);

        spriteManager = new SpriteManager(graph);
        graph.setAttribute("ui.stylesheet", stylesheet);
        graph.setAttribute("ui.antialias");
        graph.setAttribute("ui.quality");

        // Root node
        addNode("root").addAttribute("ui.label", "Join");

        // Data structures for max/average reward for join order
        numVisits = new HashMap<>();
        maxReward = new HashMap<>();
        rewardSum = new HashMap<>();
    }

    /**
     * Update the visualization for the current iteration.
     *
     * @param joinOrder        sampled join order
     * @param reward           the reward received
     * @param tupleIndices     how many tuples were examined on this join order
     * @param tableCardinality the total number of tuples per table
     */
    public void update(int[] joinOrder, double reward, int[] tupleIndices,
                       int[] tableCardinality) {
        incrementCounter();
        for (Sprite sprite : spriteManager.sprites()) {
            if (sprite.hasAttribute("progress")) {
                sprite.setAttribute("ui.hide");
            }
        }

        if (createNodesSpriteIfNotPresent(joinOrder)) {
            layout.compute();
        }

        String currentJoinNode = "";
        String previous = "root";
        for (int currentTable : joinOrder) {
            currentJoinNode += (char) (65 + currentTable);
            String spriteId = "S#" + previous + "--" + currentJoinNode;
            Sprite sprite = spriteManager.getSprite(spriteId);
            sprite.removeAttribute("ui.hide");
            sprite.setPosition(tupleIndices[currentTable] /
                    (double) tableCardinality[currentTable]);
            numVisits.put(currentJoinNode,
                    1 + (numVisits.containsKey(currentJoinNode) ?
                            numVisits.get(currentJoinNode) : 0));
            colorNode(currentJoinNode);
            previous = currentJoinNode;
        }

        updateRewardLabels(currentJoinNode, reward);

        frameTimeDelay();
    }

    /**
     * Add a node to the graph
     *
     * @param id node id
     * @return Node object
     */
    private Node addNode(String id) {
        Node node = graph.addNode(id);
        layout.nodeAdded(id);
        return node;
    }

    /**
     * Add an edge to the graph
     *
     * @param edgeId   edge id
     * @param fromId   node id of the source
     * @param toId     node id of the target
     * @param directed whether the edge is directed
     * @return Edge object
     */
    public Edge addEdge(String edgeId, String fromId, String toId,
                        boolean directed) {
        Edge edge = graph.addEdge(edgeId, fromId, toId, directed);
        layout.edgeAdded(edgeId, fromId, toId, directed);
        return edge;
    }

    /**
     * Increment the iteration counter and update the label.
     */
    private void incrementCounter() {
        iterationCounter++;
        counterLabel.setText("Number of Samples: " + iterationCounter);
    }

    /**
     * Creates the nodes/edges/join progress bars and reward labels for a
     * given join order if it doesn't exist already
     *
     * @param joinOrder the join order
     * @return whether or not the graph was modified
     */
    public boolean createNodesSpriteIfNotPresent(int[] joinOrder) {
        String currentJoinNode = "";
        String previous = "root";
        boolean modified = false;
        for (int x : joinOrder) {
            String tableName = info.aliasToTable.get(info.aliases[x]);

            currentJoinNode += (char) (65 + x);
            if (graph.getNode(currentJoinNode) == null) {
                Node newNode = addNode(currentJoinNode);
                newNode.addAttribute("ui.label", tableName);
                Edge edge = addEdge(previous + "--" + currentJoinNode,
                        previous, currentJoinNode, true);
                Sprite sprite = spriteManager.addSprite("S#" + previous +
                        "--" + currentJoinNode);
                sprite.addAttribute("ui.class", "join");
                sprite.addAttribute("progress");
                sprite.attachToEdge(edge.getId());
                sprite.setPosition(0);
                modified = true;
            }
            previous = currentJoinNode;
        }

        if (modified) {
            Sprite maxSprite = spriteManager.addSprite("SM#" + currentJoinNode);
            maxSprite.addAttribute("ui.class", "counter");
            maxSprite.attachToNode(currentJoinNode);
            maxSprite.setPosition(StyleConstants.Units.PX, 22, 138, -90);

            Sprite avgSprite = spriteManager.addSprite("SA#" + currentJoinNode);
            avgSprite.addAttribute("ui.class", "counter");
            avgSprite.attachToNode(currentJoinNode);
            avgSprite.setPosition(StyleConstants.Units.PX, 40, 250, -90);
        }

        return modified;
    }

    /**
     * Delay the animation depending on the number of samples.
     */
    private void frameTimeDelay() {
        if (iterationCounter < 5) {
            sleep(3000);
        } else if (iterationCounter < 10) {
            sleep(2000);
        } else if (iterationCounter < 15) {
            sleep(500);
        } else if (iterationCounter < 50) {
            sleep(125);
        } else if (iterationCounter < 150) {
            sleep(35);
        } else if (iterationCounter < 500) {
            sleep(10);
        }
    }

    /**
     * Color the given node depending on the number of visits
     *
     * @param node node id
     */
    private void colorNode(String node) {
        int num = Math.min(numVisits.get(node), 10000);
        long gb = 235 -
                Math.round((Math.log10(num) / 4.0) * (235 - 77));
        String color = "rgb(255, " + gb + ", " + gb + ")";
        graph.getNode(node)
                .addAttribute("ui.style", "fill-color: " + color + ";");
    }

    /**
     * Update reward labels for a given leaf node
     *
     * @param currentJoinNode the ID of the leaf node
     * @param reward          the reward for this sample
     */
    private void updateRewardLabels(String currentJoinNode, double reward) {
        if (!maxReward.containsKey(currentJoinNode)) {
            maxReward.put(currentJoinNode, reward);
        } else {
            maxReward.put(currentJoinNode, Math.max(reward,
                    maxReward.get(currentJoinNode)));
        }

        if (!rewardSum.containsKey(currentJoinNode)) {
            rewardSum.put(currentJoinNode, reward);
        } else {
            rewardSum.put(currentJoinNode, reward +
                    maxReward.get(currentJoinNode));
        }

        Sprite rewardSprite = spriteManager.getSprite("SM#" + currentJoinNode);
        rewardSprite.setAttribute("ui.label", "Max: " + String.format(
                "%6.2e", maxReward.get(currentJoinNode)));

        Sprite averageRewardSprite =
                spriteManager.getSprite("SA#" + currentJoinNode);
        double average =
                rewardSum.get(currentJoinNode) / numVisits.get(currentJoinNode);
        averageRewardSprite.setAttribute("ui.label",
                "Average: " + String.format("%6.2e", average));
    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {}
    }
}