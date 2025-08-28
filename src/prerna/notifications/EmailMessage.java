/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.notifications;

import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.util.Constants;

public class EmailMessage {

	private String from;
	private String[] to;
	private String subject;
	private String body;
	private boolean bodyIsHTML;
	private Session session;

	private static final Logger classLogger = LogManager.getLogger(EmailMessage.class);

	/**
	 * This constructor creates an email message that can be sent using the provided
	 * session.
	 *
	 * <p>
	 * The session defines the configurations for relaying messages, and is specific
	 * an organization's network. In the simplest case, the session requires an
	 * <code>SMTP_SERVER</code> and <code>SMTP_PORT</code> to relay messages. For
	 * example: <code>
	 * Properties sessionProps = new Properties();<br>
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

	/** Sends the email message. */
	public void send() {

		// Create a new message and sent it
		Message message = new MimeMessage(session);
		try {
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
		} catch (MessagingException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
}
