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
package prerna.io.connector.couch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Focused unit tests for the SSRF, path traversal, and Mango selector
 * injection hardening added to <a href="#{@link}">{@link CouchUtil}</a>. These
 * exercise the package-private helper methods directly since they are pure
 * (or filesystem-only) logic that does not require standing up CouchDB's own
 * HTTP layer. End-to-end coverage of the full download()/createDefault() flow
 * lives alongside the existing stock-image behavior tests in
 * <a href="#{@link}">{@link CouchStockImageUnitTests}</a>.
 */
class CouchUtilHardeningUnitTests {

	@TempDir
	Path temp;

	private final ObjectMapper mapper = new ObjectMapper();

	// ---- getSelectorString: Mango selector / query injection hardening ----

	@Test
	void selectorValueWithQuotesCannotInjectAdditionalSelectorClauses() throws Exception {
		// a value shaped to break out of its "$eq" string and splice in a sibling
		// "$or" clause (and a bogus "ignored" field) if it were ever concatenated
		// into the selector unescaped, the way the old implementation did
		String hostileValue = "abc\", \"$or\": [{\"admin\": true}], \"ignored\": \"z";
		Map<String, String> referenceData = new LinkedHashMap<>();
		referenceData.put("project", hostileValue);

		String selector = CouchUtil.getSelectorString(referenceData);

		// must always be valid, parseable JSON - the old hand-built concatenation
		// could easily produce malformed JSON for input shaped like this
		JsonNode parsed = mapper.readTree(selector);
		// the hostile value must come back out exactly as supplied, as a single
		// string value. The old (vulnerable) implementation would instead truncate
		// this to just "abc" and splice "$or"/"ignored" in as real JSON structure
		// alongside it - this equality check fails against that old behavior.
		assertEquals(hostileValue, parsed.path("selector").path("project").path("$eq").textValue());
	}

	@Test
	void selectorValueWithBackslashIsEscapedNotInterpreted() throws Exception {
		Map<String, String> referenceData = new LinkedHashMap<>();
		referenceData.put("insight", "back\\slash\\value");

		String selector = CouchUtil.getSelectorString(referenceData);
		JsonNode parsed = mapper.readTree(selector);

		assertEquals("back\\slash\\value", parsed.path("selector").path("insight").path("$eq").textValue());
	}

	@Test
	void selectorNullValueBecomesJsonNullNotTheWordNull() throws Exception {
		Map<String, String> referenceData = new LinkedHashMap<>();
		referenceData.put("project", null);

		String selector = CouchUtil.getSelectorString(referenceData);
		JsonNode parsed = mapper.readTree(selector);

		assertTrue(parsed.path("selector").path("project").path("$eq").isNull());
	}

	@Test
	void selectorWithMultipleFieldsBuildsAnEqClausePerField() throws Exception {
		Map<String, String> referenceData = new LinkedHashMap<>();
		referenceData.put("project", "proj-1");
		referenceData.put("insight", "insight-1");

		String selector = CouchUtil.getSelectorString(referenceData);
		JsonNode parsed = mapper.readTree(selector);

		assertEquals("proj-1", parsed.path("selector").path("project").path("$eq").textValue());
		assertEquals("insight-1", parsed.path("selector").path("insight").path("$eq").textValue());
	}

	// ---- requireWithinBase: path traversal hardening ----

	@Test
	void pathWithinBaseIsAcceptedAndCanonicalized() throws Exception {
		Path base = Files.createDirectories(temp.resolve("project-base"));
		Path within = Files.createDirectories(base.resolve("insight-id"));

		File resolved = CouchUtil.requireWithinBase(base.toString(), within.toString());

		assertEquals(within.toFile().getCanonicalFile(), resolved);
	}

	@Test
	void pathEqualToBaseItselfIsAccepted() throws Exception {
		Path base = Files.createDirectories(temp.resolve("project-base"));

		File resolved = CouchUtil.requireWithinBase(base.toString(), base.toString());

		assertEquals(base.toFile().getCanonicalFile(), resolved);
	}

	@Test
	void traversalSequenceEscapingBaseIsRejected() throws Exception {
		Path base = Files.createDirectories(temp.resolve("project-base"));
		Path outside = Files.createDirectories(temp.resolve("outside-secret"));
		// what a crafted id of "../../outside-secret" would resolve to once
		// appended to the trusted base directory
		String escapingPath = base.resolve("..").resolve(outside.getFileName()).toString();

		CouchException ex = assertThrows(CouchException.class,
				() -> CouchUtil.requireWithinBase(base.toString(), escapingPath));
		assertTrue(ex.getMessage().toLowerCase().contains("outside"));
	}

	@Test
	void siblingDirectoryWithSharedPrefixIsStillRejected() throws Exception {
		// "project-base-evil" starts with the same characters as "project-base" but
		// is not actually nested inside it - a naive String#startsWith(base) check
		// (without the trailing separator) would wrongly accept this
		Path base = Files.createDirectories(temp.resolve("project-base"));
		Path lookalike = Files.createDirectories(temp.resolve("project-base-evil"));

		assertThrows(CouchException.class, () -> CouchUtil.requireWithinBase(base.toString(), lookalike.toString()));
	}

	@Test
	void nullBaseOrCandidateSkipsValidationInsteadOfThrowing() throws Exception {
		assertNull(CouchUtil.requireWithinBase(null, temp.toString()));
		assertNull(CouchUtil.requireWithinBase(temp.toString(), null));
		assertNull(CouchUtil.requireWithinBase(null, null));
	}

	// ---- parseEndpointUri / matchesAuthority: SSRF host-pinning hardening ----

	@Test
	void blankOrMissingEndpointParsesToNull() {
		assertNull(CouchUtil.parseEndpointUri(""));
		assertNull(CouchUtil.parseEndpointUri(null));
	}

	@Test
	void endpointWithoutAHostParsesToNull() {
		// a syntactically valid but non-absolute/opaque URI has no host to pin to
		assertNull(CouchUtil.parseEndpointUri("not-a-real-endpoint"));
	}

	@Test
	void malformedEndpointParsesToNull() {
		assertNull(CouchUtil.parseEndpointUri("http://bad host with spaces/"));
	}

	@Test
	void validAbsoluteEndpointParsesWithMatchingAuthority() {
		URI parsed = CouchUtil.parseEndpointUri("http://localhost:5984/semoss/");
		assertEquals("http", parsed.getScheme());
		assertEquals("localhost", parsed.getHost());
		assertEquals(5984, parsed.getPort());
	}

	@Test
	void matchesAuthorityTrueForIdenticalSchemeHostPort() throws Exception {
		URI expected = new URI("http://localhost:5984/semoss/");
		URI request = new URI("http://localhost:5984/semoss/some-doc-id?rev=1-abc");
		assertTrue(CouchUtil.matchesAuthority(request, expected));
	}

	@Test
	void matchesAuthorityFalseForDifferentHost() throws Exception {
		URI expected = new URI("http://localhost:5984/semoss/");
		URI request = new URI("http://attacker.example:5984/semoss/some-doc-id");
		assertFalse(CouchUtil.matchesAuthority(request, expected));
	}

	@Test
	void matchesAuthorityFalseForDifferentScheme() throws Exception {
		URI expected = new URI("http://localhost:5984/semoss/");
		URI request = new URI("https://localhost:5984/semoss/some-doc-id");
		assertFalse(CouchUtil.matchesAuthority(request, expected));
	}

	@Test
	void matchesAuthorityFalseForDifferentPort() throws Exception {
		URI expected = new URI("http://localhost:5984/semoss/");
		URI request = new URI("http://localhost:9999/semoss/some-doc-id");
		assertFalse(CouchUtil.matchesAuthority(request, expected));
	}

	// ---- buildCouchUri: invalid input is rejected rather than silently sent ----

	@Test
	void buildCouchUriRejectsSuffixThatProducesAnInvalidUri() {
		// an unencoded space (and similar illegal URI characters) must not reach
		// the HTTP client silently; buildCouchUri must fail closed
		assertThrows(CouchException.class, () -> CouchUtil.buildCouchUri("bad id with spaces"));
	}
}
