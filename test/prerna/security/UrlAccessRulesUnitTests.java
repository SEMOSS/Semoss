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
package prerna.security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetAddress;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.util.Constants;
import prerna.util.InternalUtility;

class UrlAccessRulesUnitTests {

	private Properties savedCoreProp;
	private Properties rdfMap;

	@BeforeEach
	void setRdfMap() {
		this.savedCoreProp = InternalUtility.getCoreProp();
		this.rdfMap = new Properties();
		InternalUtility.setCoreProp(this.rdfMap);
	}

	@AfterEach
	void restoreRdfMap() {
		InternalUtility.setCoreProp(this.savedCoreProp);
	}

	@ParameterizedTest
	@ValueSource(strings = { "https://example.com/a", "http://10.1.2.3:8080/", "http://localhost/", "not a url" })
	void withNoListsEveryUrlPasses(String url) {
		assertDoesNotThrow(() -> UrlAccessRules.checkUrl(url));
	}

	@ParameterizedTest
	@ValueSource(strings = { "https://blocked.example/a", "https://api.blocked.example/a?x=1",
			"https://API.Blocked.Example./a", "http://internal-host:9090/", "http://deep.wild.example/" })
	void aBlacklistedDomainOrHostIsRefusedWithItsSubDomains(String url) {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "blocked.example, internal-host, *.wild.example");

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> UrlAccessRules.checkUrl(url));
		assertEquals("You are not allowed to make requests to the URL: " + url, refused.getMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = { "https://notblocked.example/a", "https://blocked.example.org/a",
			"https://example.com/blocked.example", "http://internal-host-2/" })
	void aBlacklistLeavesOtherHostsOpen(String url) {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "blocked.example, internal-host");

		assertDoesNotThrow(() -> UrlAccessRules.checkUrl(url));
	}

	@ParameterizedTest
	@ValueSource(strings = { "http://10.1.2.3/", "http://10.255.255.255:8080/x", "http://192.168.1.5/",
			"http://172.20.0.9/", "http://[fd12:3456::1]/", "http://169.254.169.254/latest/meta-data" })
	void aBlacklistedAddressOrRangeIsRefused(String url) {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS,
				"10.0.0.0/8, 172.16.0.0/12, 192.168.1.5, fc00::/7, 169.254.169.254");

		assertThrows(IllegalArgumentException.class, () -> UrlAccessRules.checkUrl(url));
	}

	@ParameterizedTest
	@ValueSource(strings = { "http://11.0.0.1/", "http://172.32.0.1/", "http://192.168.1.6/", "http://[2001:db8::1]/",
			"http://169.254.169.253/" })
	void anAddressOutsideTheBlacklistedRangesPasses(String url) {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS,
				"10.0.0.0/8, 172.16.0.0/12, 192.168.1.5, fc00::/7, 169.254.169.254");

		assertDoesNotThrow(() -> UrlAccessRules.checkUrl(url));
	}

	@Test
	void aBlacklistedRangeCoversEveryHostThatResolvesIntoIt() throws Exception {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "10.0.0.0/8");
		InetAddress[] inside = { InetAddress.getByName("93.184.216.34"), InetAddress.getByName("10.9.8.7") };
		InetAddress[] outside = { InetAddress.getByName("93.184.216.34") };

		assertThrows(IllegalArgumentException.class,
				() -> UrlAccessRules.checkAddresses("api.corp.example", inside, false));
		assertDoesNotThrow(() -> UrlAccessRules.checkAddresses("api.corp.example", outside, false));
	}

	@ParameterizedTest
	@ValueSource(strings = { "https://example.com/a", "https://api.example.com/a" })
	void aWhitelistAllowsItsDomainsAndTheirSubDomains(String url) {
		this.rdfMap.setProperty(Constants.WHITE_LIST_DOMAINS, "Example.com, googleapis.com");

		assertDoesNotThrow(() -> UrlAccessRules.checkUrl(url));
	}

	@ParameterizedTest
	@ValueSource(strings = { "https://other.org/a", "http://10.1.2.3/", "http://127.0.0.1:8080/",
			"http://internal-host/" })
	void aWhitelistRefusesEveryOtherDomainAddressAndInternalHost(String url) {
		this.rdfMap.setProperty(Constants.WHITE_LIST_DOMAINS, "example.com");

		assertThrows(IllegalArgumentException.class, () -> UrlAccessRules.checkUrl(url));
	}

	@ParameterizedTest
	@ValueSource(strings = { "https://api.example.com/a", "http://localhost:8080/run",
			"http://unoserver.default.svc.cluster.local:2003/", "http://10.0.5.7/embed", "http://[fd00::5]/" })
	void aWhitelistCanNameInternalHostsAddressesAndRanges(String url) {
		this.rdfMap.setProperty(Constants.WHITE_LIST_DOMAINS,
				"example.com, localhost, svc.cluster.local, 10.0.5.0/24, fd00::5");

		assertDoesNotThrow(() -> UrlAccessRules.checkUrl(url));
	}

	@ParameterizedTest
	@ValueSource(strings = { "https://other.org/a", "http://10.0.6.1/", "http://127.0.0.1/", "http://other-internal/",
			"http://notlocalhost/", "http://[fd00::6]/" })
	void aWhitelistWithInternalEntriesStillRefusesTheRest(String url) {
		this.rdfMap.setProperty(Constants.WHITE_LIST_DOMAINS,
				"example.com, localhost, svc.cluster.local, 10.0.5.0/24, fd00::5");

		assertThrows(IllegalArgumentException.class, () -> UrlAccessRules.checkUrl(url));
	}

	@Test
	void theBlacklistWinsOverTheWhitelist() {
		this.rdfMap.setProperty(Constants.WHITE_LIST_DOMAINS, "example.com");
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "internal.example.com");

		assertDoesNotThrow(() -> UrlAccessRules.checkUrl("https://api.example.com/a"));
		assertThrows(IllegalArgumentException.class, () -> UrlAccessRules.checkUrl("https://internal.example.com/a"));
		assertThrows(IllegalArgumentException.class, () -> UrlAccessRules.checkHost("db.internal.example.com"));
	}

	@Test
	void aUrlWithNoHostIsRefusedOnceAListIsSet() {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "blocked.example");

		assertThrows(IllegalArgumentException.class, () -> UrlAccessRules.checkUrl("not a url"));
		assertThrows(IllegalArgumentException.class, () -> UrlAccessRules.checkUrl("file:///etc/hosts"));
	}

	@Test
	void anEntryThatIsNotARangeIsIgnored() {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "10.0.0.0/99, 10.0.0.0/abc, blocked.example");

		assertDoesNotThrow(() -> UrlAccessRules.checkUrl("http://10.1.2.3/"));
		assertThrows(IllegalArgumentException.class, () -> UrlAccessRules.checkUrl("https://blocked.example/"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "169.254.169.254", "fe80::1", "0.0.0.0" })
	void aProjectReactorCannotReachTheMetadataServiceOrTheWildcardAddress(String address) throws Exception {
		InetAddress[] resolved = { InetAddress.getByName(address) };

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> UrlAccessRules.checkAddresses("some.host", resolved, true));
		assertEquals("Project reactors are not allowed to make requests to the host: some.host", refused.getMessage());
		assertDoesNotThrow(() -> UrlAccessRules.checkAddresses("some.host", resolved, false));
	}

	@ParameterizedTest
	@ValueSource(strings = { "93.184.216.34", "10.1.2.3", "192.168.1.5", "2001:db8::1", "127.0.0.1", "::1" })
	void aProjectReactorReachesOtherAddressesUnlessTheyAreBlacklisted(String address) throws Exception {
		InetAddress[] resolved = { InetAddress.getByName(address) };

		assertDoesNotThrow(() -> UrlAccessRules.checkAddresses("some.host", resolved, true));
	}

	@ParameterizedTest
	@ValueSource(strings = { "127.0.0.1", "127.8.9.10", "::1" })
	void anAdminClosesLocalhostWithTheBlacklist(String address) throws Exception {
		this.rdfMap.setProperty(Constants.BLACK_LIST_DOMAINS, "localhost, 127.0.0.0/8, ::1");
		InetAddress[] resolved = { InetAddress.getByName(address) };

		assertThrows(IllegalArgumentException.class, () -> UrlAccessRules.checkAddresses("some.host", resolved, true));
		assertThrows(IllegalArgumentException.class,
				() -> UrlAccessRules.checkAddresses("some.host", resolved, false));
		assertThrows(IllegalArgumentException.class, () -> UrlAccessRules.checkUrl("http://localhost:8080/"));
	}
}
