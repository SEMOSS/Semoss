package prerna.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

}
