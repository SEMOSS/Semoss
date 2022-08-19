package prerna.rpa.quartz.jobs.mail;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import jakarta.mail.Session;
import prerna.rpa.quartz.CommonDataKeys;

public class GetSMTPSessionJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(GetSMTPSessionJob.class.getName());
	
	/** {@code String} **/
	public static final String IN_SMTP_HOST_KEY = GetSMTPSessionJob.class + ".smtpHost";
	
	/** {@code int} **/
	public static final String IN_SMTP_PORT_KEY = GetSMTPSessionJob.class + ".smtpPort";
	
	/** {@code javax.mail.Session} */
	public static final String OUT_EMAIL_SESSION_KEY = CommonDataKeys.EMAIL_SESSION;

	private String jobName;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		jobName = context.getJobDetail().getKey().getName();
		JobDataMap dataMap = context.getMergedJobDataMap();
		String smtpHost = dataMap.getString(IN_SMTP_HOST_KEY);
		int smtpPort = dataMap.getInt(IN_SMTP_PORT_KEY);
		
		////////////////////
		// Do work
		////////////////////
	    Properties emailSessionProps = new Properties();
	    emailSessionProps.put("mail.smtp.host", smtpHost);
	    emailSessionProps.put("mail.smtp.port", Integer.toString(smtpPort));
	    Session emailSession = Session.getInstance(emailSessionProps);
		
		////////////////////
		// Store outputs
		////////////////////
	    dataMap.put(OUT_EMAIL_SESSION_KEY, emailSession);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("Received request to interrupt the " + jobName + " job. However, there is nothing to interrupt for this job.");		
	}
	
}
