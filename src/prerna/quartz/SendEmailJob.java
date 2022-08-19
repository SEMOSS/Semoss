package prerna.quartz;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import prerna.rpa.quartz.jobs.mail.EmailMessage;

public class SendEmailJob implements org.quartz.Job {

	public static final String IN_FROM_KEY = "emailFrom";
	public static final String IN_TO_KEY = "emailTo";
	public static final String IN_SUBJECT_KEY = "emailSubject";
	public static final String IN_BODY_KEY = "emailBody";
	public static final String IN_BODY_IS_HTML_KEY = "emailBodyIsHtml";
	public static final String IN_SESSION_KEY = "emailSession";

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		// Get inputs
		JobDataMap dataMap = context.getMergedJobDataMap();
		String from = dataMap.getString(IN_FROM_KEY);
		String[] to = (String[]) dataMap.get(IN_TO_KEY);
		String subject = dataMap.getString(IN_SUBJECT_KEY);
		String body = dataMap.getString(IN_BODY_KEY);
		boolean bodyIsHTML = dataMap.getBoolean(IN_BODY_IS_HTML_KEY);
		Session session = (Session) dataMap.get(IN_SESSION_KEY);

		// Do work
		EmailMessage message = new EmailMessage(from, to, subject, body, bodyIsHTML, session);
		try {
			message.send();
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Store outputs
		// No outputs to store here
	}

}
