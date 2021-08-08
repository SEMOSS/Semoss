package prerna.io.connector.couch;

public class CouchException extends Exception {

	private static final long serialVersionUID = 1L;
	
	private final Integer statusCode;

	public CouchException(String errorMessage) {
		super(errorMessage);
		statusCode = null;
	}
	
	public CouchException(Integer statusCode, String errorMessage) {
		super(errorMessage);
		this.statusCode = statusCode;
	}
	
	public CouchException(String errorMessage, Throwable cause) {
		super(errorMessage, cause);
		statusCode = null;
	}
	
	public CouchException(Integer statusCode, String errorMessage, Throwable cause) {
		super(errorMessage, cause);
		this.statusCode = statusCode;
	}
	
	public Integer getStatusCode() {
		return statusCode;
	}
}
