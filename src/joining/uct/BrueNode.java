package joining.uct;

import config.JoinConfig;
import joining.join.MultiWayJoin;
import joining.plan.JoinOrder;
import org.apache.commons.lang3.mutable.MutableBoolean;
import query.QueryInfo;

import java.util.*;

/**
 * Represents node in brue search tree.
 *
 * @author Junxiong Wang
 */
public class BrueNode extends TreeNode {
    /**
     * Assigns each action index to child node.
     */
    public BrueNode[] childNodes;
    /**
     * temporary map to store inter-middle nodes
     */
    static HashMap<JoinOrder, BrueNode> nodeMap = new HashMap<>();

    /**
     * Initialize BRUEI root node.
     *
     * @param roundCtr     current round number
     * @param query        the query which is optimized
     * @param useHeuristic whether to avoid Cartesian products
     * @param joinOp       multi-way join operator allowing fast join order switching
     */
    public BrueNode(long roundCtr, QueryInfo query,
                    boolean useHeuristic, MultiWayJoin joinOp) {
        // Count node generation
        super(roundCtr, query, useHeuristic, joinOp);
        childNodes = new BrueNode[nrActions];
    }

    /**
     * Initializes BRUEI node by expanding parent node.
     *
     * @param roundCtr    current round number
     * @param parent      parent node in UCT tree
     * @param joinedTable new joined table
     */
    public BrueNode(long roundCtr, BrueNode parent, int joinedTable) {
        super(roundCtr, parent, joinedTable);
        childNodes = new BrueNode[nrActions];
    }

    /**
     * Recursively sample from BRUEI tree and return reward.
     *
     * @param roundCtr    current round (used as timestamp for expansion)
     * @param joinOrder   partially completed join order
     * @param switchPoint switch point
     * @param expand      whether we should expand the tree
     * @param restart     whether we should reset the switch point
     * @return achieved reward
     * @throws Exception
     */
    public double sample(long roundCtr, int[] joinOrder, int switchPoint,
                         boolean expand, MutableBoolean restart) throws Exception {
        if (this.treeLevel == nrTables) {
            return joinOp.execute(joinOrder);
        }
        //pick up action for the next step
        int action = 0;
        if (this.treeLevel < switchPoint) {
            //explore the current best action
            action = explorationPolicy();
        } else {
            //select the new action
            action = estimationPolicy();
        }

        int table = nextTable[action];
        joinOrder[treeLevel] = table;
        double reward = 0;
        if (childNodes[action] != null) {
            //go to the lower level
            reward = childNodes[action].sample(roundCtr, joinOrder, switchPoint, false, restart);
        } else {
            BrueNode nextNode = null;
            if(JoinConfig.TREE_POLICY == TreeSearchPolicy.BRUEI) {
                //go the the lower level
                JoinOrder currentOrder = new JoinOrder(Arrays.copyOfRange(joinOrder, 0, treeLevel + 1));
                if (nodeMap.containsKey(currentOrder)) {
                    nextNode = nodeMap.get(currentOrder);
                } else {
                    nextNode = new BrueNode(roundCtr, this, table);
                    nodeMap.put(currentOrder, nextNode);
                }
                if (expand) {
                    //Expand the BRUE tree
                    childNodes[action] = nextNode;
                    //test whether we are currently at the switch point
                    if (this.treeLevel != switchPoint) {
                        //if the expansion is not at the switch point, mark the flag to restart the round robin
                        restart.setTrue();
                    }
                    //only expand the shallow node.
                    expand = false;
                }
            } else if(JoinConfig.TREE_POLICY == TreeSearchPolicy.BRUE) {
                //go the the next level, and follow the same strategy (exploration policy and estimation policy)
                //those intermediate nodes will not be updated except the switch point node
                childNodes[action] = new BrueNode(roundCtr, this, table);
                nextNode = childNodes[action];
            }
            reward = nextNode.sample(roundCtr, joinOrder, switchPoint, expand, restart);
        }
        /*
         * only update the reward of the node at the switch point
         */
        if (this.treeLevel == switchPoint) {
            updateStatistics(action, reward);
        }
        return reward;
    }

    /**
     * estimation policy, select best reward actions.
     * @return the selected action
     */
    private int estimationPolicy() {
        int offset = random.nextInt(nrActions);
        int bestAction = -1;
        double bestQuality = -1;
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            // Calculate index of current action
            int action = (offset + actionCtr) % nrActions;
            if (useHeuristic && !recommendedActions.contains(action))
                continue;
            double meanReward = (nrTries[action] > 0) ? accumulatedReward[action] / nrTries[action] : 0;
            //System.out.println("action:" + action);
            //System.out.println("meanReward:" + meanReward);
            if (meanReward > bestQuality) {
                bestAction = action;
                bestQuality = meanReward;
            }
        }
        return bestAction;
    }

    /**
     * exploration policy, select a random action
     * @return
     */
    private int explorationPolicy() {
        int offset = random.nextInt(nrActions);
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            int action = (offset + actionCtr) % nrActions;
            if (useHeuristic && !recommendedActions.contains(action))
                continue;
            return action;
        }
        return offset;
    }

    /**
     * Get the current optimal policy
     * @param joinOrder
     * @param roundCtr
     * @return whether we have a full order
     */
    public boolean getOptimalPolicy(int[] joinOrder, int roundCtr) {
        if (treeLevel < nrTables) {
            int action = estimationPolicy();
            int table = nextTable[action];
            joinOrder[treeLevel] = table;
            if (childNodes[action] != null)
                return childNodes[action].getOptimalPolicy(joinOrder, roundCtr);
            else {
                JoinOrder currentOrder = new JoinOrder(Arrays.copyOfRange(joinOrder, 0, treeLevel + 1));
                if (nodeMap.containsKey(currentOrder)) {
                    return nodeMap.get(currentOrder).getOptimalPolicy(joinOrder, roundCtr);
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * clear the node map
     */
    public void clearNodeMap() {
        nodeMap.clear();
    }
}
