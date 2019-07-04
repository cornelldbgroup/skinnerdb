package joining.join;

/**
 * Describes "movement" of focus during multi-way join:
 * - left: backtrack to previous table in join order
 * - right: advance to next table in join order
 * - up: reset tuple index of current table
 * - down: select next tuple for current table
 * 
 * @author immanueltrummer
 *
 */
public enum JoinMove {
	RIGHT, LEFT, UP, DOWN
}
