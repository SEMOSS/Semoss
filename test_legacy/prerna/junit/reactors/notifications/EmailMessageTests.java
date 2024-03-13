package prerna.junit.reactors.notifications;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import jakarta.mail.Session;
import prerna.notifications.EmailMessage;

public class EmailMessageTests {

	@Test
	public void testConstructor() {
		Session sess = null;
		EmailMessage em = new EmailMessage("from", new String[]{"to"}, "sub", "body", false, sess);
		assertNotNull(em);
	}
}
