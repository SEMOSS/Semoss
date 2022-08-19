package prerna.quartz.specific.tap;

import static org.quartz.JobBuilder.newJob;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;

import jakarta.mail.Session;
import prerna.quartz.JobChain;
import prerna.quartz.SendEmailJob;

public class CreateEmailJobDetails {

	private static final String SMTP_SERVER = "127.0.0.1";
	private static final int SMTP_PORT = 25;

	public JobDetail createEmailJob() {
	    List<Class<? extends Job>> jobChain = new ArrayList<Class<? extends Job>>();
	    jobChain.add(SendEmailJob.class);
	
	    //////////////////////////////
	    // Define the initial data map
	    //////////////////////////////
	    JobDataMap jobActionDataMap = new JobDataMap();
	
	    // The job sequence
	    jobActionDataMap.put(JobChain.IN_SEQUENCE, jobChain);
	
	    // Email params
	    Properties sessionProps = new Properties();
	    sessionProps.put("mail.smtp.host", SMTP_SERVER);
	    sessionProps.put("mail.smtp.port", Integer.toString(SMTP_PORT));
	    Session session = Session.getInstance(sessionProps);
	    jobActionDataMap.put(SendEmailJob.IN_FROM_KEY, "tbanach@deloitte.com");
	    jobActionDataMap.put(SendEmailJob.IN_TO_KEY, new String[] { "adaclarke@deloitte.com" });
	    jobActionDataMap.put(SendEmailJob.IN_SUBJECT_KEY, "hello world");
	    jobActionDataMap.put(SendEmailJob.IN_BODY_KEY, "Mr. Watson--come here--I want to see you");
	    jobActionDataMap.put(SendEmailJob.IN_BODY_IS_HTML_KEY, false);
	    jobActionDataMap.put(SendEmailJob.IN_SESSION_KEY, session);
	    
	    JobDetail emailJobDetail = newJob(SendEmailJob.class)
	            .usingJobData(jobActionDataMap)
	            .build();
	    
	    return emailJobDetail;
	}
}
