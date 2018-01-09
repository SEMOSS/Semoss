package prerna.rpa.config;

/**
 * Throw this exception when a user has defined a JSON job configuration that
 * cannot be parsed.
 *
 */
public class ParseConfigException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public ParseConfigException(String message) {
		super(message);
	}

	public ParseConfigException(String message, Throwable cause) {
		super(message, cause);
	}

}