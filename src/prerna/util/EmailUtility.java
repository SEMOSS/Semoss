package prerna.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.SendFailedException;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

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
			MimeBodyPart messageBodyPart = new MimeBodyPart();
			if(emailMessage == null) {
				emailMessage = "";
			}
			if(isHtml) {
				// add email message
				messageBodyPart.setContent(emailMessage, "text/html");
			} else {
				// add email message
				messageBodyPart.setText(emailMessage);
			}
			// set email message
			multipart.addBodyPart(messageBodyPart);
			
			// add attachments
			if (attachments != null) {
				for (String filePath : attachments) {
					MimeBodyPart attachmentBodyPart = new MimeBodyPart();
					try {
						attachmentBodyPart.attachFile(new File(filePath));
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Error adding attachment", e);
					}
					attachmentBodyPart.setFileName(new File(filePath).getName());
					multipart.addBodyPart(attachmentBodyPart);
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
		} catch (SendFailedException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new RuntimeException("Bad SMTP Connection");
		} catch (MessagingException me) {
			logger.error(Constants.STACKTRACE, me);
		}

		return false;
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
			emailSession  = Session.getInstance(prop, new jakarta.mail.Authenticator() {
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
