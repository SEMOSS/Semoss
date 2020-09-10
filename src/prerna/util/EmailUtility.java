package prerna.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

import com.sun.mail.smtp.SMTPSendFailedException;

public class EmailUtility {

	public static boolean sendEmail(Session emailSession, String[] recipients, String from, String subject, String emailMessage, boolean isHtml, String[] attachments) {
		try {
			// Create an email message we will add multiple parts to this
			Message email = new MimeMessage(emailSession);
			// add from
			email.setFrom(new InternetAddress(from));
			// add email recipients
			for (String recipient : recipients) {
				email.addRecipients(Message.RecipientType.TO, InternetAddress.parse(recipient));
			}
			// add email subject
			email.setSubject(subject);
			// Create a multipart message
			Multipart multipart = new MimeMultipart();

			// Create the message part
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
			// add attachments
			if (attachments != null) {
				for (String fileName : attachments) {
					MimeBodyPart messageBodyPart = new MimeBodyPart();
					DataSource source = new FileDataSource(fileName);
					messageBodyPart.setDataHandler(new DataHandler(source));
					messageBodyPart.setFileName(fileName);
					multipart.addBodyPart(messageBodyPart);
				}
			}
			// Send the complete email parts
			email.setContent(multipart);
			// Send email
			Transport.send(email);
			return true;

		} catch (SMTPSendFailedException e) {
			e.printStackTrace();
			throw new RuntimeException("Bad SMTP Connection");
		} catch (MessagingException me) {
			me.printStackTrace();
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
		boolean success = EmailUtility.sendEmail(emailSession, new String[] {"maher.khalil@va.gov"}, "VHAMSPOPMOSupport@VA.gov", "Covid Response Test", message, isHtml, null);
		if(success) {
			System.out.println("Email Sent");
		} else {
			System.out.println("Email Failed");
		}
	}

}
