package prerna.junit.reactors.auth;

import static org.junit.Assert.assertFalse;

import org.junit.Test;

import prerna.auth.User;

public class UserTests {

	@Test
	public void dropAccessTokenTest() {
		User user = new User();
		assertFalse(user.dropAccessToken("NATIVE"));
	}
}
