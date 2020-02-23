package preprocessing.uct;

import java.util.List;
import java.util.PriorityQueue;

public interface Compilable {
    List<Integer> getPredicates();
    int getAddedUtility();
    void addChildrenToCompile(PriorityQueue<Compilable> queue, int setSize);
}
