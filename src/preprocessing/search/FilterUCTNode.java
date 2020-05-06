package preprocessing.search;

import expressions.compilation.UnaryBoolEval;
import indexing.HashIndex;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static preprocessing.search.FilterSearchConfig.*;

public class FilterUCTNode {
    private enum NodeType {
        ROOT, LEAF, INDEX, BRANCHING, ROW_PARALLEL, PREDICATE_GROUPS
    }

    private static final Random random = new Random();
    private static int numPredicates;
    private static List<Integer> order = null;
    private static List<HashIndex> indices;

    // Node common members
    private final FilterUCTNode parent;
    private final long createdIn;
    private final NodeType type;
    private final int treeLevel, nrActions;
    private final int[] nrTries;
    private final int[] nrParallelSimulationsPerAction;
    private final double[] accumulatedReward;
    private int nrVisits;
    private int numRows = 0;
    private int nrParallelSimulations;
    private final List<Integer> priorityActions;

    private final int actionToPredicate[];
    private final FilterUCTNode[] childNodes;

    private final List<Integer> chosenPreds;
    private final List<Integer> unchosenPreds;


    private final int indexActions;
    private final int groupsActions;

    private int minBatches = 0;

    public FilterUCTNode(long roundCtr,
                         int numPredicates, List<HashIndex> indices) {
        this.parent = null;
        this.type = NodeType.ROOT;
        this.treeLevel = 0;
        this.createdIn = roundCtr;
        // PREAMBLE - DO NOT CHANGE

        this.numPredicates = numPredicates;
        this.indices = indices;

        int indexActions = 0;
        for (HashIndex index : indices) {
            if (index != null) {
                indexActions++;
            }
        }
        this.indexActions = indexActions;
        this.groupsActions = 0;//numPredicates - 1;

        this.nrActions = numPredicates + indexActions + groupsActions;
        this.childNodes = new FilterUCTNode[nrActions];
        this.chosenPreds = new ArrayList<>();
        this.unchosenPreds = new ArrayList<>(numPredicates);
        this.actionToPredicate = new int[nrActions];

        int pred = 0, action = numPredicates;
        for (HashIndex index : indices) {
            if (index != null) {
                actionToPredicate[action] = pred;
                action++;
            }
            pred++;
        }

        for (int i = 0; i < numPredicates; ++i) {
            unchosenPreds.add(i);
            actionToPredicate[i] = i;
        }

        /*
        for (int i = numPredicates + indexActions, s = 1;
             i < nrActions; i++) {
            actionToPredicate[i] = s++;
        }*/

        // CONCLUSION - DO NOT CHANGE
        this.nrVisits = 0;
        this.nrParallelSimulations = 0;
        this.nrTries = new int[nrActions];
        this.nrParallelSimulationsPerAction = new int[nrActions];
        this.accumulatedReward = new double[nrActions];
        this.priorityActions = new ArrayList<>(nrActions);
        for (int i = 0; i < nrActions; i++) {
            nrTries[i] = 0;
            nrParallelSimulationsPerAction[i] = 0;
            accumulatedReward[i] = 0;
            priorityActions.add(i);
        }
    }

    private FilterUCTNode(FilterUCTNode parent, long roundCtr, NodeType type) {
        assert type == NodeType.LEAF : type.name();
        this.parent = parent;
        this.type = type;
        this.treeLevel = parent.treeLevel + 1;
        this.createdIn = roundCtr;
        // PREAMBLE - DO NOT CHANGE

        this.indexActions = 0;
        this.nrActions = 0;
        this.childNodes = new FilterUCTNode[0];
        this.chosenPreds = new ArrayList<>();
        this.unchosenPreds = new ArrayList<>();
        this.actionToPredicate = new int[0];
        this.groupsActions = 0;

        // CONCLUSION - DO NOT CHANGE
        this.nrVisits = 0;
        this.nrParallelSimulations = 0;
        this.nrTries = new int[nrActions];
        this.nrParallelSimulationsPerAction = new int[nrActions];
        this.accumulatedReward = new double[nrActions];
        this.priorityActions = new ArrayList<>(nrActions);
        for (int i = 0; i < nrActions; i++) {
            nrTries[i] = 0;
            nrParallelSimulationsPerAction[i] = 0;
            accumulatedReward[i] = 0;
            priorityActions.add(i);
        }
    }

    private FilterUCTNode(FilterUCTNode parent, long roundCtr, int arg,
                          NodeType type) {
        this.parent = parent;
        this.type = type;
        this.treeLevel = parent.treeLevel + 1;
        this.createdIn = roundCtr;
        // PREAMBLE - DO NOT CHANGE

        if (type == NodeType.BRANCHING || type == NodeType.INDEX) {
            int nextPred = arg;
            this.chosenPreds = new ArrayList<>();
            this.chosenPreds.addAll(parent.chosenPreds);
            this.chosenPreds.add(nextPred);

            this.unchosenPreds = new ArrayList<>();
            this.unchosenPreds.addAll(parent.unchosenPreds);
            int indexToRemove = unchosenPreds.indexOf(nextPred);
            this.unchosenPreds.remove(indexToRemove);

            if (this.type == NodeType.INDEX) {
                List<Integer> indexedPreds = new ArrayList<>();
                int indexActions = 0;
                if (type == NodeType.INDEX) {
                    for (int pred : unchosenPreds) {
                        HashIndex index = indices.get(pred);
                        if (index != null) {
                            indexActions++;
                            indexedPreds.add(pred);
                        }
                    }
                }
                this.indexActions = indexActions;
                this.nrActions = unchosenPreds.size() + this.indexActions;

                this.childNodes = new FilterUCTNode[nrActions];
                this.actionToPredicate = new int[nrActions];

                for (int a = unchosenPreds.size(), i = 0; a < nrActions; ++a) {
                    actionToPredicate[a] = indexedPreds.get(i++);
                }
            } else {
                this.indexActions = 0;
                this.nrActions = unchosenPreds.size();
                this.childNodes = new FilterUCTNode[nrActions];
                this.actionToPredicate = new int[nrActions];
            }


            for (int a = 0; a < unchosenPreds.size(); ++a) {
                actionToPredicate[a] = unchosenPreds.get(a);
            }
        } else {
            assert type == NodeType.ROW_PARALLEL : type.name();
            this.minBatches = arg;
            this.nrActions = ROW_PARALLEL_ACTIONS;
            this.childNodes = new FilterUCTNode[nrActions];
            this.chosenPreds = new ArrayList<>();
            this.unchosenPreds = new ArrayList<>();
            this.actionToPredicate = new int[0];
            this.indexActions = 0;
        }

        this.groupsActions = 0;

        // CONCLUSION - DO NOT CHANGE
        this.nrVisits = 0;
        this.nrParallelSimulations = 0;
        this.nrTries = new int[nrActions];
        this.nrParallelSimulationsPerAction = new int[nrActions];
        this.accumulatedReward = new double[nrActions];
        this.priorityActions = new ArrayList<>(nrActions);
        for (int i = 0; i < nrActions; i++) {
            nrTries[i] = 0;
            nrParallelSimulationsPerAction[i] = 0;
            accumulatedReward[i] = 0;
            priorityActions.add(i);
        }
    }

    public Pair<FilterUCTNode, Boolean> sample(long roundCtr,
                                               FilterState state,
                                               Map<List<Integer>,
                                                       UnaryBoolEval> cache) {
        if (type == NodeType.LEAF) {
            return Pair.of(this, false);
        }


        boolean canExpand = createdIn != roundCtr;
        int action = selectAction();

        switch (type) {
            case ROOT: {
                if (action == numPredicates + indexActions) {
                    state.avoidBranching = true;
                    if (childNodes[action] == null && canExpand) {
                        childNodes[action] = new FilterUCTNode(this, roundCtr,
                                NodeType.LEAF);
                    }
                } else {
                    int predicate = actionToPredicate[action];
                    state.order[treeLevel] = predicate;
                    order = new ArrayList<>();
                    order.add(predicate);

                    if (this.indexActions > 0 &&
                            action >= unchosenPreds.size() &&
                            action < unchosenPreds.size() + indexActions) {
                        state.indexedTil = treeLevel;
                        order = new ArrayList<>();
                        state.cachedTil = -1;
                        state.cachedEval = null;
                    }

                    if (childNodes[action] == null && canExpand) {
                        if (this.unchosenPreds.size() == 1) {
                            if (ENABLE_ROW_PARALLELISM) {
                                childNodes[action] = new FilterUCTNode(this,
                                        roundCtr, 1,
                                        NodeType.ROW_PARALLEL);
                            } else {
                                childNodes[action] = new FilterUCTNode(this,
                                        roundCtr, NodeType.LEAF);
                            }
                        } else if (state.indexedTil == treeLevel) {
                            childNodes[action] = new FilterUCTNode(this,
                                    roundCtr, predicate, NodeType.INDEX);
                        } else {
                            childNodes[action] = new FilterUCTNode(this,
                                    roundCtr, predicate, NodeType.BRANCHING);
                        }
                    }
                }

                break;
            }

            case INDEX:
            case BRANCHING: {
                int predicate = actionToPredicate[action];
                state.order[treeLevel] = predicate;
                order.add(predicate);
                if (cache.get(order) != null) {
                    state.cachedTil = treeLevel;
                    state.cachedEval = cache.get(order);
                }

                if (this.indexActions > 0 &&
                        action >= unchosenPreds.size() &&
                        action < unchosenPreds.size() + indexActions) {
                    state.indexedTil = treeLevel;
                    order = new ArrayList<>();
                    state.cachedTil = -1;
                    state.cachedEval = null;
                }

                if (childNodes[action] == null && canExpand) {
                    if (this.unchosenPreds.size() == 1) {
                        if (ENABLE_ROW_PARALLELISM) {
                            childNodes[action] = new FilterUCTNode(this,
                                    roundCtr, 1,
                                    NodeType.ROW_PARALLEL);
                        } else {
                            childNodes[action] = new FilterUCTNode(this,
                                    roundCtr, NodeType.LEAF);
                        }
                    } else {
                        if (state.indexedTil == treeLevel) {
                            childNodes[action] = new FilterUCTNode(this,
                                    roundCtr, predicate, NodeType.INDEX);
                        } else {
                            childNodes[action] = new FilterUCTNode(this,
                                    roundCtr, predicate, NodeType.BRANCHING);
                        }
                    }
                }
                break;
            }

            case ROW_PARALLEL: {
                state.batches =
                        minBatches + action * ROW_PARALLEL_DELTA;

                if (childNodes[action] == null && canExpand) {
                    if (action == nrActions - 1) {
                        childNodes[action] = new FilterUCTNode(this,
                                roundCtr, state.batches,
                                NodeType.ROW_PARALLEL);
                    } else {
                        childNodes[action] = new FilterUCTNode(this, roundCtr,
                                NodeType.LEAF);
                    }
                }
                break;
            }
        }

        FilterUCTNode child = childNodes[action];
        if (child == null) {
            playout(state);
            return Pair.of(this, true);
        } else {
            state.actions.add(action);
            return child.sample(roundCtr, state, cache);
        }
    }

    private void playout(FilterState state) {
        switch (type) {
            case BRANCHING:
            case INDEX: {
                int lastPred = state.order[treeLevel];

                Collections.shuffle(unchosenPreds);
                Iterator<Integer> unchosenPredsIter = unchosenPreds.iterator();
                for (int i = treeLevel + 1; i < numPredicates; ++i) {
                    int nextPred = unchosenPredsIter.next();
                    while (nextPred == lastPred) {
                        nextPred = unchosenPredsIter.next();
                    }
                    state.order[i] = nextPred;
                }

                // Use playouts that have no parallel batches to avoid spending
                // a large time on bad orders
                state.batches = 1;
                break;
            }

            case ROW_PARALLEL: {
                // no additional work needed
                break;
            }

            case LEAF:
                throw new RuntimeException("Not possible to playout from leaf");

            case ROOT:
                throw new RuntimeException("Not possible to playout from root");
        }
    }


    // Common UCT functions
    private int selectAction() {
        if (!priorityActions.isEmpty()) {
            int nrUntried = priorityActions.size();
            int actionIndex = random.nextInt(nrUntried);
            int action = priorityActions.get(actionIndex);
            // Remove from untried actions and return
            priorityActions.remove(actionIndex);
            // System.out.println("Untried action: " + action);
            return action;
        }

        int offset = random.nextInt(nrActions);
        int bestAction = -1;
        double bestQuality = -1;
        for (int actionCtr = 0; actionCtr < nrActions; ++actionCtr) {
            // Calculate index of current action
            int action = (offset + actionCtr) % nrActions;
            if (nrTries[action] == 0) continue;
            double meanReward = accumulatedReward[action] / nrTries[action];
            double exploration =
                    Math.sqrt(Math.log(nrVisits + nrParallelSimulations) /
                            (nrTries[action] +
                                    nrParallelSimulationsPerAction[action]));
            // Assess the quality of the action according to policy
            double quality = meanReward +
                    FilterSearchConfig.EXPLORATION_FACTOR * exploration;

            if (quality > bestQuality) {
                bestAction = action;
                bestQuality = quality;
            }
        }

        if (bestAction < 0) {
            return random.nextInt(nrActions);
        }

        // Otherwise: return best action.
        return bestAction;
    }

    public void initializeUtility(HashMap<FilterUCTNode, Integer> utility,
                                  Map<List<Integer>,
                                          UnaryBoolEval> cache) {
        if (this.type == NodeType.ROOT) {
            for (int a = 0; a < Math.min(nrActions, numPredicates); ++a) {
                if (this.childNodes[a] != null) {
                    this.childNodes[a].initializeUtility(utility, cache);
                }
            }
        } else {
            for (int a = 0; a < nrActions; ++a) {
                if (this.childNodes[a] != null) {
                    if (!cache.containsKey(this.chosenPreds)) {
                        utility.put(this.childNodes[a],
                                this.chosenPreds.size() * this.numRows);
                    }

                    this.childNodes[a].initializeUtility(utility, cache);
                }
            }
        }

    }

    public void updateUtility(HashMap<FilterUCTNode, Integer> utility,
                              Map<List<Integer>,
                                      UnaryBoolEval> cache) {
        FilterUCTNode parent = this.parent;
        while (parent != null) {
            if (parent.parent == null || cache.containsKey(parent.chosenPreds)) {
                break;
            }

            utility.put(parent,
                    utility.get(parent) - parent.chosenPreds.size() * numRows);
            parent = parent.parent;
        }

        Stack<FilterUCTNode> children = new Stack<>();
        for (int a = 0; a < nrActions; ++a) {
            if (this.childNodes[a] != null &&
                    !cache.containsKey(this.childNodes[a])) {
                children.push(this.childNodes[a]);
            }
        }
        while (!children.empty()) {
            FilterUCTNode curr = children.pop();
            int saved = utility.get(curr);
            utility.put(curr,
                    saved - (this.chosenPreds.size() - curr.chosenPreds.size())
                            * curr.numRows);
            for (int a = 0; a < nrActions; ++a) {
                if (curr.childNodes[a] != null &&
                        !cache.containsKey(curr.childNodes[a])) {
                    children.push(curr.childNodes[a]);
                }
            }
        }
    }

    public static void finalUpdateStatistics(FilterUCTNode node,
                                             FilterState state,
                                             double reward) {
        int numRows = state.batches * state.batchSize;
        int i = state.actions.size() - 1;
        while (node != null) {
            ++node.nrVisits;
            --node.nrParallelSimulations;
            node.numRows += numRows;

            if (i >= 0) {
                int selectedAction = state.actions.get(i--);
                ++node.parent.nrTries[selectedAction];
                --node.parent.nrParallelSimulationsPerAction[selectedAction];
                node.parent.accumulatedReward[selectedAction] += reward;
            }

            node = node.parent;
        }
    }

    public static void initialUpdateStatistics(FilterUCTNode node,
                                               FilterState state) {
        int i = state.actions.size() - 1;
        while (node != null) {
            ++node.nrParallelSimulations;

            if (i >= 0) {
                int selectedAction = state.actions.get(i--);
                ++node.parent.nrParallelSimulationsPerAction[selectedAction];
            }

            node = node.parent;
        }
    }

    // Getters
    public List<Integer> getChosenPreds() {
        return chosenPreds;
    }

}
