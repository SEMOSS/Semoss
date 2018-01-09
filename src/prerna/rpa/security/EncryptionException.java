package prerna.rpa.security;

/**
 * Throw this exception when unable to encrypt or decrypt.
 *
 */
public class EncryptionException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public EncryptionException(Throwable cause) {
		super(cause);
	}
	
	public EncryptionException(String message) {
		super(message);
	}

	public EncryptionException(String message, Throwable cause) {
		super(message, cause);
	}

}