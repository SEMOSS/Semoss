package prerna.rpa.reporting;

public class ReportProcessingException  extends Exception {

	private static final long serialVersionUID = 1L;

	public ReportProcessingException(String message) {
		super(message);
	}

	public ReportProcessingException(String message, Throwable cause) {
		super(message, cause);
	}

}
