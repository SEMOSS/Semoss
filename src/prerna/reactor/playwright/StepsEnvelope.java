package prerna.reactor.playwright;

import java.util.List;

public record StepsEnvelope(
        String version,
        RecordingMeta meta,
        List<List<Step>> steps
) {}
