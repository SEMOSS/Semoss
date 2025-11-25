package prerna.reactor.playwright;

/**
 * Represents a selector used to identify an element on a webpage. This record
 * is an immutable data carrier for the strategy and value of a selector.
 *
 * @param strategy A string indicating the method used to select the element
 *                 (e.g., "id", "css", "xpath", "text", "role", "testId",
 *                 "placeholder", "label").
 * @param value    The actual value of the selector (e.g., "myElementId",
 *                 ".my-class", "//div[@id='foo']").
 */
public record Selector(String strategy, String value) {
}
