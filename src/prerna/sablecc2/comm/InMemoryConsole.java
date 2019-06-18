package prerna.sablecc2.comm;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class InMemoryConsole extends Logger {
	
	// the default is mute
	public enum LOG_LEVEL {BASIC, INFO, DEBUG, WARN, FATAL, MUTE};
	private LOG_LEVEL level = LOG_LEVEL.BASIC;

	private String jobID;
	
	/**
	 * need to write some kind of output
	 * need a way to say where the recipe is in the operation - this needs to be done by translation to say
	 * 2 of 30 or something like that
	 * need a way to mute a particular queue
	 * need a way to adjust the level of logging I am trying to get
	 * 
	 * The message needs to be pass
	 * 
	 * 
	 */

	public InMemoryConsole(String jobId, String className) {
		super(className);
		this.jobID = jobId;
		// set the repository
		this.repository = LogManager.getLoggerRepository();
		// set the level
		setLevel(Level.INFO);
		//configure the appender
		ConsoleAppender console = new ConsoleAppender(); //create appender
		String PATTERN = "%d [%p|%c{1.}] %m%n";
		console.setLayout(new EnhancedPatternLayout(PATTERN)); 
		console.setThreshold(Level.INFO);
		console.activateOptions();
		//add appender to the logger
		addAppender(console);
	}

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
	public void info(Object message)
	{
		super.info(message);
		if(level == LOG_LEVEL.INFO || level == LOG_LEVEL.DEBUG || level == LOG_LEVEL.WARN || level == LOG_LEVEL.FATAL) {
			JobManager.getManager().addStdOut(jobID, message + "");
		}
	}
	
	@Override
	public void debug(Object message) {
		super.debug(message);
		if(level == LOG_LEVEL.DEBUG || level == LOG_LEVEL.WARN || level == LOG_LEVEL.FATAL) {
			JobManager.getManager().addStdErr(jobID, message + "");
		}
	}
	
	@Override
	public void warn(Object message)
	{
		super.warn(message);
		if(level == LOG_LEVEL.WARN || level == LOG_LEVEL.FATAL)
		{
			JobManager.getManager().addStdErr(jobID, message + "");
		}
	}

	@Override
	public void fatal(Object message)
	{
		super.fatal(message);
		if(level == LOG_LEVEL.FATAL)
		{
			JobManager.getManager().addStdErr(jobID, message + "");
		}
	}
}
