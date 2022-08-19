package prerna.rpa.quartz.jobs.mail;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import prerna.rpa.quartz.CommonDataKeys;
import prerna.util.Utility;

public class SendEmailJob implements org.quartz.InterruptableJob {

	private static final Logger logger = LogManager.getLogger(SendEmailJob.class);

	/** {@code String} **/
	public static final String IN_FROM_KEY = SendEmailJob.class + ".from";
	
	/** {@code String[]} **/
	public static final String IN_TO_KEY = SendEmailJob.class + ".to";
	
	/** {@code String} **/
	public static final String IN_SUBJECT_KEY = SendEmailJob.class + ".subject";
	
	/** {@code String} **/
	public static final String IN_BODY_KEY = SendEmailJob.class + ".body";
	
	/** {@code boolean} **/
	public static final String IN_BODY_IS_HTML_KEY = SendEmailJob.class + ".bodyIsHTML";
	
	/** {@code javax.mail.Session} **/
	public static final String IN_EMAIL_SESSION_KEY = CommonDataKeys.EMAIL_SESSION;

	private String jobName;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		////////////////////
		// Get inputs
		////////////////////
		jobName = context.getJobDetail().getKey().getName();
		JobDataMap dataMap = context.getMergedJobDataMap();
		String from = dataMap.getString(IN_FROM_KEY);
		String[] to = (String[]) dataMap.get(IN_TO_KEY);
		String subject = dataMap.getString(IN_SUBJECT_KEY);
		String body = dataMap.getString(IN_BODY_KEY);
		boolean bodyIsHTML = dataMap.getBoolean(IN_BODY_IS_HTML_KEY);
		Session session = (Session) dataMap.get(IN_EMAIL_SESSION_KEY);

		////////////////////
		// Do work
		////////////////////
		EmailMessage message = new EmailMessage(from, to, subject, body, bodyIsHTML, session);
		logger.info(Utility.cleanLogString(message.toString()));
		try {
			message.send();
		} catch (MessagingException e) {
			String sendEmailExceptionMessage = "Failed to send the email with subject " + subject + ".";
			logger.error(sendEmailExceptionMessage);
			throw new JobExecutionException(sendEmailExceptionMessage, e);
		}

		////////////////////
		// Store outputs
		////////////////////
		// No outputs to store here
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		logger.warn("Received request to interrupt the " + jobName + " job. However, there is nothing to interrupt for this job.");
	}

}
