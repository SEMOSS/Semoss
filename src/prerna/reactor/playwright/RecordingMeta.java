package prerna.reactor.playwright;

/**
 * Represents metadata for a Playwright recording. This record is an immutable
 * data carrier for descriptive and temporal information about a recorded
 * script.
 *
 * @param id          A stable, unique identifier for the script (optional).
 * @param title       An optional short, human-readable title for the recording.
 * @param description A more detailed description of the recording.
 * @param createdAt   The timestamp (epoch milliseconds) when the recording was
 *                    created.
 * @param updatedAt   The timestamp (epoch milliseconds) when the recording was
 *                    last updated.
 * @param intent      An optional string describing the intent or purpose of the recording.
 */
public record RecordingMeta(String id, String title, String description, Long createdAt, Long updatedAt, String intent) {
}