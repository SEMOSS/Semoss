package prerna.reactor.agent.run;

import java.util.Locale;

/**
 * Controls how a subagent run reports completion to its parent run.
 *
 * <p>
 * {@link #WAIT}: the parent collects the result with WaitForSubAgent.
 * {@link #POST}: the result is posted to the parent room.
 * {@link #POST_AND_CONTINUE}: the result is posted and one new parent-room run starts.
 */
public enum SubAgentRunCompletionMode {

	WAIT,
	POST,
	POST_AND_CONTINUE;

	public static SubAgentRunCompletionMode fromExternalValue(String value) {
		if (value == null || value.trim().isEmpty()) {
			return WAIT;
		}
		return valueOf(value.trim().toUpperCase(Locale.ROOT));
	}

	/**
	 * Read the persisted value without allowing old or unknown data to opt into
	 * asynchronous behavior. Requests written before this field existed therefore
	 * retain the WAIT behavior.
	 */
	public static SubAgentRunCompletionMode fromPersistedValue(Object value) {
		if (value == null) {
			return WAIT;
		}
		try {
			return fromExternalValue(String.valueOf(value));
		} catch (IllegalArgumentException ignored) {
			return WAIT;
		}
	}
}
