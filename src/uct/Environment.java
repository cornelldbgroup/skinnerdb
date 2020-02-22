package uct;

public interface Environment<T extends Action> {
    double execute(int budget, T action);
    boolean isFinished();
}
