/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.ldap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

import org.junit.jupiter.api.Test;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;

class LDAPConnectionHelperUnitTests {

	// 100ns units between 1601-01-01 and 1970-01-01 (the Windows FILETIME epoch offset)
	private static final long FILETIME_EPOCH_OFFSET = 116444736000000000L;

	private static long toWindowsFileTime(long epochMillis) {
		return (epochMillis * 10000L) + FILETIME_EPOCH_OFFSET;
	}

	// ---- convertWinFileTimeToJava ----

	@Test
	void testConvertWinFileTimeToJava_epochZero() {
		ZonedDateTime result = LDAPConnectionHelper.convertWinFileTimeToJava(FILETIME_EPOCH_OFFSET);
		// Compare the represented instant rather than full ZonedDateTime equality: the
		// production code resolves the zone via TimeZone.getTimeZone("UTC").toZoneId(),
		// a named zone region, which is a distinct ZoneId from ZoneOffset.UTC even though
		// both represent the same moment in time.
		assertEquals(Instant.EPOCH, result.toInstant());
	}

	@Test
	void testConvertWinFileTimeToJava_stringOverloadMatchesLongOverload() {
		long fileTime = toWindowsFileTime(1_000_000_000_000L);
		ZonedDateTime fromLong = LDAPConnectionHelper.convertWinFileTimeToJava(fileTime);
		ZonedDateTime fromString = LDAPConnectionHelper.convertWinFileTimeToJava(Long.toString(fileTime));
		assertEquals(fromLong, fromString);
	}

	// ---- getAttributeValue ----

	@Test
	void testGetAttributeValue_nullNameReturnsNull() throws Exception {
		Attributes attrs = new BasicAttributes();
		assertNull(LDAPConnectionHelper.getAttributeValue(attrs, null));
	}

	@Test
	void testGetAttributeValue_missingAttributeReturnsNull() throws Exception {
		Attributes attrs = new BasicAttributes();
		assertNull(LDAPConnectionHelper.getAttributeValue(attrs, "doesNotExist"));
	}

	@Test
	void testGetAttributeValue_presentAttributeReturnsValue() throws Exception {
		Attributes attrs = new BasicAttributes();
		attrs.put(new BasicAttribute("mail", "user@example.com"));
		assertEquals("user@example.com", LDAPConnectionHelper.getAttributeValue(attrs, "mail"));
	}

	// ---- getLastPwdChange ----

	@Test
	void testGetLastPwdChange_noKeyDefinedReturnsNull() throws Exception {
		Attributes attrs = new BasicAttributes();
		assertNull(LDAPConnectionHelper.getLastPwdChange(attrs, null, 0));
		assertNull(LDAPConnectionHelper.getLastPwdChange(attrs, "", 0));
	}

	@Test
	void testGetLastPwdChange_missingAttributeThrowsIllegalArgument() {
		Attributes attrs = new BasicAttributes();
		assertThrows(IllegalArgumentException.class,
				() -> LDAPConnectionHelper.getLastPwdChange(attrs, "pwdLastSet", 0));
	}

	@Test
	void testGetLastPwdChange_sentinelZeroRequiresPasswordChange() {
		Attributes attrs = new BasicAttributes();
		attrs.put(new BasicAttribute("pwdLastSet", "0"));
		assertThrows(LDAPPasswordChangeRequiredException.class,
				() -> LDAPConnectionHelper.getLastPwdChange(attrs, "pwdLastSet", 0));
	}

	@Test
	void testGetLastPwdChange_sentinelZeroAsIntegerAlsoRequiresChange() {
		Attributes attrs = new BasicAttributes();
		attrs.put(new BasicAttribute("pwdLastSet", 0));
		// requirePwdChangeAfterDays irrelevant - the "never changed" sentinel always wins
		assertThrows(LDAPPasswordChangeRequiredException.class,
				() -> LDAPConnectionHelper.getLastPwdChange(attrs, "pwdLastSet", 30));
	}

	@Test
	void testGetLastPwdChange_noAgeRequirementReturnsParsedDate() throws Exception {
		Attributes attrs = new BasicAttributes();
		attrs.put(new BasicAttribute("pwdLastSet", Long.toString(FILETIME_EPOCH_OFFSET)));
		ZonedDateTime result = LDAPConnectionHelper.getLastPwdChange(attrs, "pwdLastSet", 0);
		assertEquals(Instant.EPOCH, result.toInstant());
	}

	@Test
	void testGetLastPwdChange_recentChangeDoesNotRequireUpdate() throws Exception {
		long yesterdayMillis = System.currentTimeMillis() - (24L * 60 * 60 * 1000);
		Attributes attrs = new BasicAttributes();
		attrs.put(new BasicAttribute("pwdLastSet", toWindowsFileTime(yesterdayMillis)));
		ZonedDateTime result = LDAPConnectionHelper.getLastPwdChange(attrs, "pwdLastSet", 90);
		assertNotNull(result);
	}

	@Test
	void testGetLastPwdChange_oldChangeRequiresUpdate() {
		// epoch (1970) is far more than 90 days before "now"
		Attributes attrs = new BasicAttributes();
		attrs.put(new BasicAttribute("pwdLastSet", FILETIME_EPOCH_OFFSET));
		assertThrows(LDAPPasswordChangeRequiredException.class,
				() -> LDAPConnectionHelper.getLastPwdChange(attrs, "pwdLastSet", 90));
	}

	@Test
	void testGetLastPwdChange_unhandledTypeWithNoRequirementReturnsNull() throws Exception {
		Attributes attrs = new BasicAttributes();
		// a Double is neither Integer, Long, nor String - falls into the "unhandled" warn branch
		attrs.put(new BasicAttribute("pwdLastSet", 123.45));
		assertNull(LDAPConnectionHelper.getLastPwdChange(attrs, "pwdLastSet", 0));
	}

	@Test
	void testGetLastPwdChange_unhandledTypeWithRequirementThrowsIllegalArgument() {
		Attributes attrs = new BasicAttributes();
		attrs.put(new BasicAttribute("pwdLastSet", 123.45));
		assertThrows(IllegalArgumentException.class,
				() -> LDAPConnectionHelper.getLastPwdChange(attrs, "pwdLastSet", 30));
	}

	// ---- toUnicodePassword ----

	@Test
	void testToUnicodePassword_wrapsInQuotesAndEncodesUtf16LE() throws Exception {
		byte[] result = LDAPConnectionHelper.toUnicodePassword("secret");
		byte[] expected = "\"secret\"".getBytes(StandardCharsets.UTF_16LE);
		assertTrue(java.util.Arrays.equals(expected, result));
	}

	@Test
	void testToUnicodePassword_emptyPassword() throws Exception {
		byte[] result = LDAPConnectionHelper.toUnicodePassword("");
		byte[] expected = "\"\"".getBytes(StandardCharsets.UTF_16LE);
		assertTrue(java.util.Arrays.equals(expected, result));
	}

	// ---- generateAccessToken ----

	@Test
	void testGenerateAccessToken_missingIdThrows() {
		Attributes attrs = new BasicAttributes();
		assertThrows(IllegalArgumentException.class, () -> LDAPConnectionHelper.generateAccessToken(
				attrs, "cn=user,dc=example,dc=com", "uid", "cn", "mail", "sAMAccountName", null, 0, true));
	}

	@Test
	void testGenerateAccessToken_populatesFieldsAndIgnoresPwdCheck() throws Exception {
		Attributes attrs = new BasicAttributes();
		attrs.put(new BasicAttribute("uid", "jdoe"));
		attrs.put(new BasicAttribute("cn", "Jane Doe"));
		attrs.put(new BasicAttribute("mail", "jane.doe@example.com"));
		attrs.put(new BasicAttribute("sAMAccountName", "jdoe"));

		AccessToken token = LDAPConnectionHelper.generateAccessToken(attrs, "cn=Jane Doe,dc=example,dc=com",
				"uid", "cn", "mail", "sAMAccountName", "pwdLastSet", 0, true);

		assertEquals(AuthProvider.LDAP, token.getProvider());
		assertEquals("jdoe", token.getId());
		assertEquals("Jane Doe", token.getName());
		assertEquals("jane.doe@example.com", token.getEmail());
		assertEquals("jdoe", token.getUsername());
		assertNull(token.getLastPasswordReset());
	}

	@Test
	void testGenerateAccessToken_sentinelZeroStillThrowsWhenNotIgnored() {
		Attributes attrs = new BasicAttributes();
		attrs.put(new BasicAttribute("uid", "jdoe"));
		attrs.put(new BasicAttribute("pwdLastSet", "0"));

		assertThrows(LDAPPasswordChangeRequiredException.class, () -> LDAPConnectionHelper.generateAccessToken(
				attrs, "cn=Jane Doe,dc=example,dc=com", "uid", null, null, null, "pwdLastSet", 0, false));
	}

}
