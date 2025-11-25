package prerna.reactor.playwright;

/**
 * Represents a single variable extracted from a Playwright script, typically
 * from TYPE or VARIABLE steps. This record is an immutable data carrier.
 *
 * @param label      The label or name of the variable (e.g., "username",
 *                   "password").
 * @param text       The value associated with the variable.
 * @param isPassword A boolean indicating if the variable represents a password,
 *                   which might imply special handling (e.g., masking in UI).
 */
public record VariableRecord(String label, String text, boolean isPassword) {

	/**
	 * Convenience constructor that defaults {@code isPassword} to {@code false}.
	 * 
	 * @param label The label or name of the variable.
	 * @param text  The value associated with the variable.
	 */
	public VariableRecord(String label, String text) {
		this(label, text, false);
	}

}
