package prerna.error;

@SuppressWarnings("serial")
public class InvalidUploadFormatException extends Exception {
	public InvalidUploadFormatException() {
		
	}
	
	public InvalidUploadFormatException(String message) {
		super(message);
	}
}
