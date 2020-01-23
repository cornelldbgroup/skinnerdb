package search;

import java.util.List;

public interface Agent<T> {
    double simulate(List<T> action);
}
