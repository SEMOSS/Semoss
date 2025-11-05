package prerna.sablecc2.comm;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import prerna.util.Utility;

public class InMemoryConsole extends Logger {

	private String jobID;
	private boolean partial;

	// Store the FQCN of this class to help Log4j identify the correct caller
	private static final String FQCN = InMemoryConsole.class.getName();

	public InMemoryConsole(String name, String jobId) {
		super((LoggerContext) LogManager.getContext(false), name, null);
		this.jobID = jobId;
		setLevel(Level.INFO);
	}

	public void setPartial(boolean partial) {
		this.partial = partial;
	}

	public void setJobID(String jobID) {
		this.jobID = jobID;
	}

	@Override
	public void info(String message) {
		if (isEnabled(Level.INFO)) {
			String cleanedMessage = Utility.cleanLogString(message);
			// Use the log method with FQCN to preserve caller information
			logMessage(FQCN, Level.INFO, null, cleanedMessage);
			if (partial) {
				PixelJobManager.getManager().addPartialOut(jobID, message + "");
			} else {
				PixelJobManager.getManager().addStdOut(jobID, message + "");
			}
		}
	}

	@Override
	public void debug(String message) {
		if (isEnabled(Level.DEBUG)) {
			String cleanedMessage = Utility.cleanLogString(message);
			// Use the log method with FQCN to preserve caller information
			logMessage(FQCN, Level.DEBUG, null, cleanedMessage);
			PixelJobManager.getManager().addStdErr(jobID, message + "");
		}
	}

	@Override
	public void warn(String message) {
		if (isEnabled(Level.WARN)) {
			String cleanedMessage = Utility.cleanLogString(message);
			logMessage(FQCN, Level.WARN, null, cleanedMessage);
			PixelJobManager.getManager().addStdErr(jobID, message + "");
		}
	}

	@Override
	public void fatal(String message) {
		if (isEnabled(Level.FATAL)) {
			String cleanedMessage = Utility.cleanLogString(message);
			logMessage(FQCN, Level.FATAL, null, cleanedMessage);
			PixelJobManager.getManager().addStdErr(jobID, message + "");
		}
	}
}