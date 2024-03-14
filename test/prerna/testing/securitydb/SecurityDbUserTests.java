package prerna.testing.securitydb;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.utils.SecurityNativeUserUtils;
import prerna.testing.AbstractBaseSemossApiTests;

@TestMethodOrder(OrderAnnotation.class)
public class SecurityDbUserTests extends AbstractBaseSemossApiTests {

	private static final String NATIVE_DUMMY_USER_ID = "DUMMY_USER_123";
	private static final AuthProvider NATIVE_DUMMY_USER_PROVIDER = AuthProvider.NATIVE;
	private static final String NATIVE_DUMMY_USERNAME = "DUMMYUSERNAME";
	private static final String NATIVE_DUMMY_EMAIL = "example@mail.com";
	private static final String NATIVE_DUMMY_PASSWORD = "SEMoss@123123!@#";
	
	@Override
	@BeforeEach
	public void beforeEachTest() {
		this.clearAllDatabasesBetweenTests = false;
		super.beforeEachTest();
	}

	@Test
	@Order(1)
	public void createNativeUser() {
		AccessToken newUser = new AccessToken();
		newUser.setId(NATIVE_DUMMY_USER_ID);
		newUser.setProvider(NATIVE_DUMMY_USER_PROVIDER);
		newUser.setUsername(NATIVE_DUMMY_USERNAME);
		newUser.setEmail(NATIVE_DUMMY_EMAIL);
		try {
			assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, NATIVE_DUMMY_PASSWORD));
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
}
