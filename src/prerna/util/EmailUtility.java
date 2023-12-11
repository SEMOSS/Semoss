package prerna.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Flags;
import jakarta.mail.Folder;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.NoSuchProviderException;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.SendFailedException;
import jakarta.mail.Session;
import jakarta.mail.Store;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.search.FlagTerm;
import prerna.usertracking.UserTrackingUtils;

public class EmailUtility {

	private static final Logger logger = LogManager.getLogger(EmailUtility.class);

	/**
	 * 
	 * @param emailSession
	 * @param toRecipients
	 * @param ccRecipients
	 * @param bccRecipients
	 * @param from
	 * @param subject
	 * @param emailMessage
	 * @param isHtml
	 * @param attachments
	 * @return
	 */
	public static boolean sendEmail(Session emailSession, String[] toRecipients, String[] ccRecipients, String[] bccRecipients, 
			String from, String subject, String emailMessage, boolean isHtml, String[] attachments) {
		
		boolean successful = doSendEmail(emailSession, toRecipients, ccRecipients, bccRecipients, from, subject, emailMessage, isHtml, attachments);
		UserTrackingUtils.trackEmail(toRecipients, ccRecipients, bccRecipients, from, subject, emailMessage, isHtml, attachments, successful);
		return successful;
	}

	/**
	 * 
	 * @param emailSession
	 * @param toRecipients
	 * @param ccRecipients
	 * @param bccRecipients
	 * @param from
	 * @param subject
	 * @param emailMessage
	 * @param isHtml
	 * @param attachments
	 * @return
	 */
	private static boolean doSendEmail(Session emailSession, String[] toRecipients, String[] ccRecipients,
			String[] bccRecipients, String from, String subject, String emailMessage, boolean isHtml,
			String[] attachments) {
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

	
	
	
	
	
	
	
	
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////

	/**
	 * @throws MessagingException 
	 * 
	 */
	public static void readEmailPOP3() throws MessagingException {
		try {
			Properties pop3EmailProps = new Properties();
			pop3EmailProps.put("mail.pop3.host", "pop.gmail.com");
			pop3EmailProps.put("mail.pop3.port", "995");
			pop3EmailProps.put("mail.pop3.starttls.enable", "true");
			pop3EmailProps.put("mail.store.protocol", "pop3");

			String username = "ncrt.test.email@gmail.com";
			String password = "";
			
			Session emailSession = null;
			try {
				if (username != null && password != null) {
					logger.info("Making secured connection to the email server");
					emailSession = Session.getInstance(pop3EmailProps, new jakarta.mail.Authenticator() {
						protected PasswordAuthentication getPasswordAuthentication() {
							return new PasswordAuthentication(username, password);
						}
					});
				} else {
					logger.info("Making connection to the email server");
					emailSession = Session.getInstance(pop3EmailProps);
				}
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occurred connecting to the email session defined. Please ensure the proper settings are set for connecting. Detailed error: " + e.getMessage(), e);
			}

			Store store = null;
			try {
				//create the POP3 store object and connect with the pop server
				store = emailSession.getStore("pop3s");
				store.connect();
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occurred establishing the pop3 connection. Please ensure the proper settings are set for connecting. Detailed error: " + e.getMessage(), e);
			}
			
			//create the folder object and open it
			Folder emailFolder = store.getFolder("INBOX");
			emailFolder.open(Folder.READ_ONLY);

			// retrieve the messages from the folder in an array and print it
			Message[] messages = emailFolder.getMessages();
			System.out.println("messages.length---" + messages.length);

			for (int i = 0, n = messages.length; i < n; i++) {
				Message message = messages[i];
				System.out.println("---------------------------------");
				System.out.println("Email Number " + (i + 1));
				System.out.println("Subject: " + message.getSubject());
				System.out.println("From: " + message.getFrom()[0]);
				System.out.println("Text: " + message.getContent().toString());

			}

			//close the store and folder objects
			emailFolder.close(false);
			store.close();

		} catch (NoSuchProviderException e) {
			e.printStackTrace();
		} catch (MessagingException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}



	}
	
	/**
	 * @throws MessagingException 
	 * 
	 */
	public static void readEmailIMAP() throws MessagingException {
		Properties props = System.getProperties();
		props.setProperty("mail.store.protocol", "imaps");
		try {
			Session session = Session.getDefaultInstance(props, null);
			Store store = session.getStore("imaps");
			store.connect("imap.gmail.com", "ncrt.test.email@gmail.com", "");

			Folder inbox = store.getFolder("Inbox");
			// setting it seen is considered a write operation
			inbox.open(Folder.READ_WRITE);
			//Message messages[] = inbox.getMessages();
			FlagTerm ft = new FlagTerm(new Flags(Flags.Flag.SEEN), false);
			Message messages[] = inbox.search(ft);

			for (int i = 0, n = messages.length; i < n; i++) {
				Message message = messages[i];
				System.out.println("---------------------------------");
				System.out.println("Email Number " + (i + 1));
				System.out.println("Subject: " + message.getSubject());
				System.out.println("From: " + message.getFrom()[0]);
				System.out.println("Text: " + message.getContent().toString());
				
				// this will mark as seen
				message.setFlag(Flags.Flag.SEEN, true);
			}
			
			//close the store and folder objects
			inbox.close(false);
			store.close();
		} catch (NoSuchProviderException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (MessagingException e) {
			e.printStackTrace();
			System.exit(2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	

//	public static void main(String[] args) throws Exception {
//		readEmailPOP3();
////		readEmailIMAP();
//		//		// GMAIL
//		//		String smtpHost = "smtp.gmail.com";
//		//		String smtpPort = "465";
//		//		String username = "ncrt.test.email@gmail.com";
//		//		String password = "pmpbgpvzhkptsijc";
//		//
//		//		Properties props = new Properties();
//		//		props.put("mail.smtp.host", smtpHost);
//		//		props.put("mail.smtp.port", smtpPort);
//		//		props.put("mail.smtp.socketFactory.port", smtpPort);
//		//		props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
//		//		Session emailSession = null;
//		//		if (username != null && password != null) {
//		//			props.put("mail.smtp.auth", true);
//		//			props.put("mail.smtp.starttls.enable", true);
//		//			System.out.println("Making connection");
//		//			emailSession  = Session.getInstance(props, new javax.mail.Authenticator() {
//		//				protected PasswordAuthentication getPasswordAuthentication() {
//		//					return new PasswordAuthentication(username, password);
//		//				}
//		//			});
//		//		} else {
//		//			System.out.println("Making connection");
//		//			emailSession = Session.getInstance(props);
//		//		}
//		//
//		//		String message = "<html><h1 style=\"color:blue;\">Covid</h1><p>Here is an html paragraph :)</p></html>";
//		//		boolean isHtml = true;
//		//
//		//		System.out.println("Connection Made");
//		//		boolean success = EmailUtility.sendEmail(emailSession, new String[] {"ncrt.test.email@gmail.com"}, "VHAMSPOPMOSupport@VA.gov", "Covid Response Test", message, isHtml, null);
//		//		if(success) {
//		//			System.out.println("Email Sent");
//		//		} else {
//		//			System.out.println("Email Failed");
//		//		}
//
////		Properties prop = Utility.loadProperties("P:/emailProperties.properties");
////		String username = prop.getProperty("username");
////		//		if(username == null) {
////		//			username = "VHAMSPOPMOSupport";
////		//		}
////		String password = prop.getProperty("password");
////		//		if(password == null) {
////		//			password = "P@ssword1P@ssword1"; 
////		//		}
////
////		Session emailSession = null;
////		if (username != null && password != null) {
////			System.out.println("Making connection");
////			emailSession  = Session.getInstance(prop, new jakarta.mail.Authenticator() {
////				protected PasswordAuthentication getPasswordAuthentication() {
////					return new PasswordAuthentication(username, password);
////				}
////			});
////		} else {
////			System.out.println("Making connection");
////			emailSession = Session.getInstance(prop);
////		}
////
////		String message = "<html><h1 style=\"color:blue;\">Covid</h1><p>Here is an html paragraph :)</p></html>";
////		boolean isHtml = true;
////
////		System.out.println("Connection Made");
////		boolean success = EmailUtility.sendEmail(emailSession, new String[] {"khalil.maher91@gmail.com"}, null, null, "VHAMSPOPMOSupport@VA.gov", "Covid Response Test", message, isHtml, null);
////		if(success) {
////			System.out.println("Email Sent");
////		} else {
////			System.out.println("Email Failed");
////		}
//	}

}
