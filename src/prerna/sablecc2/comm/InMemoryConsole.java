package prerna.sablecc2.comm;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import prerna.util.Utility;

public class InMemoryConsole extends Logger {
	
	// the default is mute
	public enum LOG_LEVEL {BASIC, INFO, DEBUG, WARN, FATAL, MUTE};
	private LOG_LEVEL level = LOG_LEVEL.BASIC;

	private String jobID;
	
	public InMemoryConsole(String name, String jobId) {
		super((LoggerContext) LogManager.getContext(false), name, null);
		this.jobID = jobId;
		// set the repository
		// set the level
		setLevel(Level.INFO);
	}
	
//	public InMemoryConsole(String jobId, String className) {
//		super(className);
//		this.jobID = jobId;
//		// set the repository
//		this.repository = LogManager.getLoggerRepository();
//		// set the level
//		setLevel(Level.INFO);
//		//configure the appender
//		ConsoleAppender console = new ConsoleAppender(); //create appender
//		String PATTERN = "%d [%p|%c{1.}] %m%n";
//		console.setLayout(new EnhancedPatternLayout(PATTERN)); 
//		console.setThreshold(Level.INFO);
//		console.activateOptions();
//		//add appender to the logger
//		addAppender(console);
//	}

	public void setJobID(String jobID) {
		this.jobID = jobID;
	}
	
	@Override
	public void setLevel(Level level) {
		if(level == Level.INFO) {
			this.level = LOG_LEVEL.INFO;
		} else if(level == Level.DEBUG) {
			this.level = LOG_LEVEL.WARN;
		} else if(level == Level.FATAL) {
			this.level = LOG_LEVEL.FATAL;
		} else if(level == Level.OFF) {
			this.level = LOG_LEVEL.BASIC;
		}
		super.setLevel(level);
	}
	
	@Override
	public void info(String message)
	{
		super.info(this.jobID + " >> " + Utility.cleanLogString(message.toString()));
		if(level == LOG_LEVEL.INFO || level == LOG_LEVEL.DEBUG || level == LOG_LEVEL.WARN || level == LOG_LEVEL.FATAL) {
			JobManager.getManager().addStdOut(jobID, message + "");
		}
	}
	
	@Override
	public void debug(String message) {
		super.debug(this.jobID + " >> " + Utility.cleanLogString(message));
		if(level == LOG_LEVEL.DEBUG || level == LOG_LEVEL.WARN || level == LOG_LEVEL.FATAL) {
			JobManager.getManager().addStdErr(jobID, message + "");
		}
	}
	
	@Override
	public void warn(String message)
	{
		super.warn(this.jobID + " >> " + Utility.cleanLogString(message.toString()));
		if(level == LOG_LEVEL.WARN || level == LOG_LEVEL.FATAL)
		{
			JobManager.getManager().addStdErr(jobID, message + "");
		}
	}

	@Override
	public void fatal(String message)
	{
		super.fatal(this.jobID + " >> " + message);
		if(level == LOG_LEVEL.FATAL)
		{
			JobManager.getManager().addStdErr(jobID, message + "");
		}
	}
}
