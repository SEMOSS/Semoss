package prerna.reactor.playwright;

/**
 * Represents the response from a screenshot operation, containing the captured
 * image and its dimensions. This record is an immutable data carrier.
 *
 * @param base64Png         The Base64 encoded string of the captured PNG image.
 * @param width             The width of the captured screenshot in pixels.
 * @param height            The height of the captured screenshot in pixels.
 * @param deviceScaleFactor The device pixel ratio (DPR) at which the screenshot
 *                          was taken.
 */
public record ScreenshotResponse(String base64Png, int width, int height, double deviceScaleFactor) {
}
