package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class SecurityTokenUtilsUnitTests extends AbstractSecurityUtilsUnitTests {

	@Test
	void testClearExpiredTokens() {
		String ipAddr = "ipAddress";
		String clientId = "clientId";
		Object[] tokenObj = SecurityTokenUtils.generateToken(ipAddr, clientId);
		SecurityTokenUtils.clearExpiredTokens(0);
		// test invalid token
		tokenObj = SecurityTokenUtils.getToken(ipAddr);
		assertNull(tokenObj);
	}

	@Test
	void testGenerateToken() {
		String ipAddr = "ipAddress";
		String clientId = "clientId";
		Object[] tokenObj = SecurityTokenUtils.generateToken(ipAddr, clientId);
		String tokenValue = (String) tokenObj[0];
		String ipActual = (String) tokenObj[1];
		String cliendIdActual = (String) tokenObj[2];

		assertTrue(tokenValue != null && !tokenValue.isEmpty());
		assertEquals(ipAddr, ipActual);
		assertEquals(clientId, cliendIdActual);
	}

	@Test
	void testGetToken() {
		String ipAddr = "ipAddress";
		String clientId = "clientId";
		SecurityTokenUtils.generateToken(ipAddr, clientId);
		Object[] tokenObj = SecurityTokenUtils.getToken(ipAddr);
		String tokenValue = (String) tokenObj[0];
		String ipActual = (String) tokenObj[1];
		String cliendIdActual = (String) tokenObj[2];

		assertTrue(tokenValue != null && !tokenValue.isEmpty());
		assertEquals(ipAddr, ipActual);
		assertEquals(clientId, cliendIdActual);

		// test invalid token
		tokenObj = SecurityTokenUtils.getToken("badIP");
		assertNull(tokenObj);
	}
}
