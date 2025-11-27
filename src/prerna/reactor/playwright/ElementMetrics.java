package prerna.reactor.playwright;

/**
 * Represents various dimension-related metrics of an HTML element. This record
 * is an immutable data carrier for properties like offset, client, and scroll
 * dimensions.
 *
 * @param offsetWidth  The offset width of the element.
 * @param offsetHeight The offset height of the element.
 * @param clientWidth  The client width of the element.
 * @param clientHeight The client height of the element.
 * @param scrollWidth  The scroll width of the element.
 * @param scrollHeight The scroll height of the element.
 */
public record ElementMetrics(int offsetWidth, int offsetHeight, int clientWidth, int clientHeight, int scrollWidth,
		int scrollHeight) {
}