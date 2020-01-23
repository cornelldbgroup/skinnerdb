package search;

import java.util.List;

public interface Agent<T extends Action> {
    double simulate(List<T> action);
}
