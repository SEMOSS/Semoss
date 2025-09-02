package prerna.reactor.playwright;

public record RecordingMeta(
        String id,                 // stable id for the script (optional)
        String title,              // optional short title
        String description,        // <-- what you asked for
        Long createdAt,            // epoch millis
        Long updatedAt             // epoch millis
) {}