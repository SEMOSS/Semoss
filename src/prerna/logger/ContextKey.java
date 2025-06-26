package prerna.logger;

public enum ContextKey {
    ENGINE_ID("engineId"),
    ENGINE_NAME("engineName"),
    ENGINE_TYPE("engineType"),
    PROJECT_ID("projectId"),
    PROJECT_NAME("projectName"),
    INSIGHT_ID("insightId"),
    PIXEL_ID("pixelId"),
	REACTOR_NAME("reactorName");

    private final String contextKey;

    ContextKey(String contextKey) {
        this.contextKey = contextKey;
    }

    public String getContextKey() {
        return contextKey;
    }
}
