package prerna.rpa;

/**
 * Throw this exception when the program encounters an unrecoverable error that
 * prevents it from initializing correctly.
 *
 */
public class FailedToInitializeException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public FailedToInitializeException(String message) {
		super(message);
	}

	public FailedToInitializeException(String message, Throwable cause) {
		super(message, cause);
	}

}
