package prerna.reactor.agent.run;

import java.util.Locale;

/**
 * Controls how a subagent run reports completion to its parent run.
 *
 * <p>
 * Only {@link #JOIN} has execution semantics today. The other values establish
 * the durable request contract used by later asynchronous-delivery work.
 */
public enum SubAgentRunCompletionMode {

	JOIN,
	NOTIFY,
	CONTINUE;

	public static SubAgentRunCompletionMode fromExternalValue(String value) {
		if (value == null || value.trim().isEmpty()) {
			return JOIN;
		}
		return valueOf(value.trim().toUpperCase(Locale.ROOT));
	}

	/**
	 * Read the persisted value without allowing old or unknown data to opt into
	 * asynchronous behavior. Requests written before this field existed therefore
	 * retain the current JOIN behavior.
	 */
	public static SubAgentRunCompletionMode fromPersistedValue(Object value) {
		if (value == null) {
			return JOIN;
		}
		try {
			return fromExternalValue(String.valueOf(value));
		} catch (IllegalArgumentException ignored) {
			return JOIN;
		}
	}
}
