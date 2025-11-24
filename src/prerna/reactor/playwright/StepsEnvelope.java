package prerna.reactor.playwright;

import java.util.List;
import java.util.Map;

public record StepsEnvelope(String version, RecordingMeta meta, Map<String, List<List<PlaywrightStep>>> steps) {
}
