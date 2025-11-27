package prerna.reactor.playwright;

/**
 * Represents the dimensions and device pixel ratio of a browser viewport. This
 * record is an immutable data carrier.
 *
 * @param width             The width of the viewport in pixels.
 * @param height            The height of the viewport in pixels.
 * @param deviceScaleFactor The device pixel ratio (DPR) of the viewport.
 */
public record Viewport(int width, int height, double deviceScaleFactor) {
}
