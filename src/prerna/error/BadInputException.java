package prerna.error;

@SuppressWarnings("serial")
public class BadInputException extends Exception{
	public BadInputException() {

	}

	public BadInputException(String message) {
		super(message);
	}
}
