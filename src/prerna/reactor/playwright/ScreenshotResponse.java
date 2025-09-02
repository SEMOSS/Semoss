package prerna.reactor.playwright;

public record ScreenshotResponse(String base64Png, int width, int height, double deviceScaleFactor) {}

