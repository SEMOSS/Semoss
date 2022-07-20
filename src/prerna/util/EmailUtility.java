package prerna.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sun.mail.smtp.SMTPSendFailedException;

public class EmailUtility {

	private static final Logger logger = LogManager.getLogger(EmailUtility.class);
	
	public static boolean sendEmail(Session emailSession, String[] toRecipients, String[] ccRecipients, String[] bccRecipients, 
			String from, String subject, String emailMessage, boolean isHtml, String[] attachments) {
		
		if ( (toRecipients == null || toRecipients.length == 0)
				&& (ccRecipients == null || ccRecipients.length == 0)
				&& (bccRecipients == null || bccRecipients.length == 0)
				) {
			logger.info("No receipients to send an email to");
			return false;
		}
		
		try {
			// Create an email message we will add multiple parts to this
			Message email = new MimeMessage(emailSession);
			// add from
			email.setFrom(new InternetAddress(from));
			// add email recipients
			if (toRecipients != null) {
				for (String recipient : toRecipients) {
					email.addRecipients(Message.RecipientType.TO, InternetAddress.parse(recipient));
				}
			}
			if (ccRecipients != null) {
				for (String recipient : ccRecipients) {
					email.addRecipients(Message.RecipientType.CC, InternetAddress.parse(recipient));
				}
			}
			if (bccRecipients != null) {
				for (String recipient : bccRecipients) {
					email.addRecipients(Message.RecipientType.BCC, InternetAddress.parse(recipient));
				}
			}
			// add email subject
			email.setSubject(subject);
			// Create a multipart message
			Multipart multipart = new MimeMultipart();

			// Create the message part
			if(emailMessage != null) {
				if(isHtml) {
					BodyPart messageBodyPart = new MimeBodyPart();
					// add email message
					messageBodyPart.setDataHandler(new DataHandler(new HTMLDataSource(emailMessage)));
					// Set email message
					multipart.addBodyPart(messageBodyPart);
				} else {
					BodyPart messageBodyPart = new MimeBodyPart();
					// add email message
					messageBodyPart.setText(emailMessage);
					// Set email message
					multipart.addBodyPart(messageBodyPart);
				}
			} else {
				// add an empty body
				BodyPart messageBodyPart = new MimeBodyPart();
				// add email message
				messageBodyPart.setText("");
				// Set email message
				multipart.addBodyPart(messageBodyPart);
			}
			// add attachments
			if (attachments != null) {
				for (String fileName : attachments) {
					MimeBodyPart messageBodyPart = new MimeBodyPart();
					DataSource source = new FileDataSource(fileName);
					messageBodyPart.setDataHandler(new DataHandler(source));
					messageBodyPart.setFileName(new File(fileName).getName());
					multipart.addBodyPart(messageBodyPart);
				}
			}
			// Send the complete email parts
			email.setContent(multipart);
			// Send email
			Transport.send(email);
			// Log email
			StringBuilder logMessage = new StringBuilder("Email subject = '" + subject)
					.append("' has been sent: ");
			if(toRecipients != null) {
				logMessage.append("to ").append(Arrays.toString(toRecipients)).append(". ");
			}
			if(ccRecipients != null) {
				logMessage.append("cc ").append(Arrays.toString(ccRecipients)).append(". ");
			}
			if(bccRecipients != null) {
				logMessage.append("bcc ").append(Arrays.toString(bccRecipients)).append(". ");
			}
			logger.info(logMessage.toString());
			
			return true;
		} catch (SMTPSendFailedException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new RuntimeException("Bad SMTP Connection");
		} catch (MessagingException me) {
			logger.error(Constants.STACKTRACE, me);
		}

		return false;
	}

	static class HTMLDataSource implements DataSource {

		private String html;

		public HTMLDataSource(String htmlString) {
			html = htmlString;
		}

		@Override
		public InputStream getInputStream() throws IOException {
			if (html == null) {
				throw new IOException("html message is null!");
			}
			return new ByteArrayInputStream(html.getBytes());
		}

		@Override
		public OutputStream getOutputStream() throws IOException {
			throw new IOException("This DataHandler cannot write HTML");
		}

		@Override
		public String getContentType() {
			return "text/html";
		}

		@Override
		public String getName() {
			return "HTMLDataSource";
		}
	}

	/**
	 * Replace dynamic components in the message
	 * @param emailTemplate
	 * @param customReplacements
	 * @return
	 */
	public static String fillEmailComponents(String emailTemplate, Map<String, String> customReplacements) {
		if (customReplacements != null && !customReplacements.isEmpty()) {
			for (Map.Entry<String, String> entry : customReplacements.entrySet()) {
				String key = entry.getKey();
				String replacementValue = entry.getValue();
				emailTemplate = emailTemplate.replace(key, replacementValue);
			}
		}
		
		return emailTemplate;
	}
	
	public static void main(String[] args) {

//		// GMAIL
//		String smtpHost = "smtp.gmail.com";
//		String smtpPort = "465";
//		String username = "ncrt.test.email@gmail.com";
//		String password = "pmpbgpvzhkptsijc";
//
//		Properties props = new Properties();
//		props.put("mail.smtp.host", smtpHost);
//		props.put("mail.smtp.port", smtpPort);
//		props.put("mail.smtp.socketFactory.port", smtpPort);
//		props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
//		Session emailSession = null;
//		if (username != null && password != null) {
//			props.put("mail.smtp.auth", true);
//			props.put("mail.smtp.starttls.enable", true);
//			System.out.println("Making connection");
//			emailSession  = Session.getInstance(props, new javax.mail.Authenticator() {
//				protected PasswordAuthentication getPasswordAuthentication() {
//					return new PasswordAuthentication(username, password);
//				}
//			});
//		} else {
//			System.out.println("Making connection");
//			emailSession = Session.getInstance(props);
//		}
//
//		String message = "<html><h1 style=\"color:blue;\">Covid</h1><p>Here is an html paragraph :)</p></html>";
//		boolean isHtml = true;
//
//		System.out.println("Connection Made");
//		boolean success = EmailUtility.sendEmail(emailSession, new String[] {"ncrt.test.email@gmail.com"}, "VHAMSPOPMOSupport@VA.gov", "Covid Response Test", message, isHtml, null);
//		if(success) {
//			System.out.println("Email Sent");
//		} else {
//			System.out.println("Email Failed");
//		}
		
		Properties prop = Utility.loadProperties("P:/emailProperties.properties");
		String username = prop.getProperty("username");
//		if(username == null) {
//			username = "VHAMSPOPMOSupport";
//		}
		String password = prop.getProperty("password");
//		if(password == null) {
//			password = "P@ssword1P@ssword1"; 
//		}
		
		Session emailSession = null;
		if (username != null && password != null) {
			System.out.println("Making connection");
			emailSession  = Session.getInstance(prop, new javax.mail.Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(username, password);
				}
			});
		} else {
			System.out.println("Making connection");
			emailSession = Session.getInstance(prop);
		}

		String message = "<html><h1 style=\"color:blue;\">Covid</h1><p>Here is an html paragraph :)</p></html>";
		boolean isHtml = true;

		System.out.println("Connection Made");
		boolean success = EmailUtility.sendEmail(emailSession, new String[] {"khalil.maher91@gmail.com"}, null, null, "VHAMSPOPMOSupport@VA.gov", "Covid Response Test", message, isHtml, null);
		if(success) {
			System.out.println("Email Sent");
		} else {
			System.out.println("Email Failed");
		}
	}

}
