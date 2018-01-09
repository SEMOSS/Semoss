package prerna.rpa.config;

/**
 * Throw this exception when a user has defined a JSON job configuration that
 * can be parsed, but contains business logic that is not permitted.
 *
 */
public class IllegalConfigException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public IllegalConfigException(String message) {
		super(message);
	}

	public IllegalConfigException(String message, Throwable cause) {
		super(message, cause);
	}

}
