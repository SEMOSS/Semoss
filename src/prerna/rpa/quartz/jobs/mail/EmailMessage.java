package prerna.rpa.quartz.jobs.mail;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class EmailMessage {

	private static final String NEW_LINE = System.getProperty("line.separator");
	
	private String from;
	private String[] to;
	private String subject;
	private String body;
	private boolean bodyIsHTML;
	private Session session;

	/**
	 * <p>
	 * This constructor creates an email message that can be sent using the
	 * provided session.
	 * </p>
	 * 
	 * <p>
	 * The session defines the configurations for relaying messages, and is
	 * specific an organization's network. In the simplest case, the session
	 * requires an <code>SMTP_SERVER</code> and <code>SMTP_PORT</code> to relay
	 * messages. For example:
	 * </p>
	 * 
	 * <code>Properties sessionProps = new Properties();<br>
	 * sessionProps.put("mail.smtp.host", SMTP_SERVER);<br>
	 * sessionProps.put("mail.smtp.port", Integer.toString(SMTP_PORT));<br>
	 * Session session = Session.getInstance(sessionProps);<br>
	 * </code>
	 * 
	 * @param from
	 *            email address
	 * @param to
	 *            email address
	 * @param subject
	 *            as plain text
	 * @param body
	 *            as either html or plain text
	 * @param bodyIsHTML
	 *            whether the body should be sent as html
	 * @param session
	 *            the session defines, at a minimum, the server and port used to
	 *            relay email messages
	 */
	public EmailMessage(String from, String[] to, String subject, String body, boolean bodyIsHTML, Session session) {
		this.from = from;
		this.to = to;
		this.subject = subject;
		this.body = body;
		this.bodyIsHTML = bodyIsHTML;
		this.session = session;
	}

	/**
	 * Sends the email message.
	 * @throws MessagingException  
	 */
	public void send() throws MessagingException {

		// Create a new message and sent it
		Message message = new MimeMessage(session);
		message.setFrom(new InternetAddress(from));
		for (String recipient : to) {
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
		}
		message.setSubject(subject);

		// If the body of the email is not HTML, then send as plain text
		if (bodyIsHTML) {
			message.setContent(body, "text/html; charset=utf-8");
		} else {
			message.setText(body);
		}
		Transport.send(message);
	}
	
	@Override
	public String toString() {
		StringBuilder emailMessageString = new StringBuilder();
		emailMessageString.append(NEW_LINE);
		emailMessageString.append("from: " + from + NEW_LINE);
		emailMessageString.append("to: ");
		for (String t : to) {
			emailMessageString.append(t + NEW_LINE);
		}
		emailMessageString.append(NEW_LINE);
		emailMessageString.append(subject);
		emailMessageString.append(NEW_LINE);
		emailMessageString.append(NEW_LINE);
		emailMessageString.append(body);
		emailMessageString.append(NEW_LINE);
		return emailMessageString.toString();
	}

}
