package prerna.reactor.playwright;

import java.util.List;
import java.util.Map;

/**
 * Acts as a container for a complete Playwright recording, encapsulating all
 * necessary information about a recorded sequence of actions. This record is an
 * immutable data carrier.
 *
 * @param version A string indicating the version of the recording format.
 * @param meta    A {@link RecordingMeta} object containing metadata about the
 *                recording (e.g., ID, title, description, timestamps).
 * @param steps   A nested map representing the actual recorded steps. The outer
 *                map's keys are {@code tabId}s, and its values are
 *                {@code List<List<PlaywrightStep>>}. This structure allows for
 *                organizing steps by tab and then by "page" (where a new inner
 *                list might represent steps on a new page load or significant
 *                page change).
 */
public record StepsEnvelope(String version, RecordingMeta meta, Map<String, List<List<PlaywrightStep>>> steps) {
}
