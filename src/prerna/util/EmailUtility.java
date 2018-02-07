package prerna.util;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import com.sun.mail.smtp.SMTPSendFailedException;

public class EmailUtility {

	public static boolean sendEmail(Session emailSession, String[] recipients, String from, String subject,
			String emailMessage, String[] attachments) {
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
			// Create the message part
			BodyPart messageBodyPart = new MimeBodyPart();
			// add email message
			messageBodyPart.setText(emailMessage);
			// Create a multipart message
			Multipart multipart = new MimeMultipart();
			// Set email message
			multipart.addBodyPart(messageBodyPart);
			// add attachments
			if (attachments != null) {
				for (String fileName : attachments) {
					messageBodyPart = new MimeBodyPart();
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
			throw new RuntimeException("Bad SMTP Connection");
		} catch (MessagingException me) {
			me.printStackTrace();
		}

		return false;
	}

}
