package prerna.reactor.playwright;

/**
 * Represents the rectangular dimensions and position of an element in a 2D
 * space. This record is an immutable data carrier for properties defining a
 * bounding box.
 *
 * @param x      The x-coordinate of the top-left corner of the rectangle.
 * @param y      The y-coordinate of the top-left corner of the rectangle.
 * @param width  The width of the rectangle.
 * @param height The height of the rectangle.
 */
public record ElementRect(double x, double y, double width, double height) {
}
