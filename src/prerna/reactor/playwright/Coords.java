package prerna.reactor.playwright;

/**
 * Represents a 2D coordinate with integer x and y values. This record is an
 * immutable data carrier for points on a screen or element.
 *
 * @param x The x-coordinate.
 * @param y The y-coordinate.
 */
public record Coords(int x, int y) {
}