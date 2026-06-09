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
package prerna.reactor.playwright;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddVisionContextReactorTest {

	private AddVisionContextReactor reactor;

	@Mock
	private Insight mockInsight;

	@Mock
	private NounStore mockNounStore;

	@BeforeEach
	public void setUp() {
		MockitoAnnotations.openMocks(this);
		reactor = new AddVisionContextReactor();
		reactor.setInsight(mockInsight);
		reactor.setNounStore(mockNounStore);
		// Initialize keyValue map for test execution
		reactor.keyValue = new java.util.HashMap<>();
	}

	@Test
	public void testExecute_WithValidVisionContext() {
		String testContext = "Vision context for testing";
		reactor.keyValue.put("visionContext", testContext);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(testContext, result.getValue());
		assertEquals(PixelDataType.CONST_STRING, result.getNounType());
	}

	@Test
	public void testExecute_WithNullVisionContext_ThrowsException() {
		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	public void testExecute_WithEmptyVisionContext() {
		reactor.keyValue.put("visionContext", "");

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals("", result.getValue());
		assertEquals(PixelDataType.CONST_STRING, result.getNounType());
	}

	@Test
	public void testExecute_WithComplexVisionContext() {
		String complexContext = "{\n  \"elements\": [\n    {\"id\": \"button-1\", \"type\": \"button\"}\n  ]\n}";
		reactor.keyValue.put("visionContext", complexContext);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(complexContext, result.getValue());
		assertEquals(PixelDataType.CONST_STRING, result.getNounType());
	}

	@Test
	public void testGetReactorDescription() {
		String description = reactor.getReactorDescription();
		assertNotNull(description);
		assertFalse(description.isEmpty());
		assertTrue(description.contains("vision") || description.contains("context"));
	}

	@Test
	public void testGetDescriptionForKey() {
		String keyDescription = reactor.getDescriptionForKey("visionContext");
		assertNotNull(keyDescription);
		assertFalse(keyDescription.isEmpty());
	}

	@Test
	public void testKeysToGet() {
		String[] expectedKeys = { "visionContext" };
		assertArrayEquals(expectedKeys, reactor.keysToGet);
	}

	@Test
	public void testMissingRequiredKey_ThrowsException() {
		assertThrows(IllegalArgumentException.class, () -> reactor.execute());
	}

	@Test
	public void testVisionContextWithSpecialCharacters() {
		String contextWithSpecialChars = "Context with @#$%^&*() special chars!";
		reactor.keyValue.put("visionContext", contextWithSpecialChars);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(contextWithSpecialChars, result.getValue());
	}

	@Test
	public void testVisionContextWithUnicodeCharacters() {
		String unicodeContext = "Context with unicode: 你好 مرحبا Привет";
		reactor.keyValue.put("visionContext", unicodeContext);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(unicodeContext, result.getValue());
	}

	@Test
	public void testVisionContextWithLongString() {
		String longString = "This is a long context string. ".repeat(100);
		reactor.keyValue.put("visionContext", longString);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(longString, result.getValue());
	}

	@Test
	public void testVisionContextWithNewlines() {
		String contextWithNewlines = "Line 1\nLine 2\nLine 3\nLine 4";
		reactor.keyValue.put("visionContext", contextWithNewlines);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(contextWithNewlines, result.getValue());
	}

	@Test
	public void testVisionContextWithTabs() {
		String contextWithTabs = "Column1\tColumn2\tColumn3";
		reactor.keyValue.put("visionContext", contextWithTabs);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(contextWithTabs, result.getValue());
	}

	@Test
	public void testVisionContextWithWhitespaceOnly() {
		String whitespace = "   \t\n   ";
		reactor.keyValue.put("visionContext", whitespace);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(whitespace, result.getValue());
	}

	@Test
	public void testVisionContextWithHTMLContent() {
		String htmlContent = "<div class='container'><button id='btn'>Click Me</button></div>";
		reactor.keyValue.put("visionContext", htmlContent);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(htmlContent, result.getValue());
	}

	@Test
	public void testVisionContextWithXMLContent() {
		String xmlContent = "<?xml version=\"1.0\"?><root><element>value</element></root>";
		reactor.keyValue.put("visionContext", xmlContent);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(xmlContent, result.getValue());
	}

	@Test
	public void testVisionContextWithJSONArray() {
		String jsonArray = "[{\"type\":\"button\",\"id\":\"btn1\"},{\"type\":\"input\",\"id\":\"input1\"}]";
		reactor.keyValue.put("visionContext", jsonArray);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(jsonArray, result.getValue());
	}

	@Test
	public void testVisionContextWithEscapedCharacters() {
		String escapedContent = "Text with \\\"quotes\\\" and \\n newlines \\t tabs";
		reactor.keyValue.put("visionContext", escapedContent);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(escapedContent, result.getValue());
	}

	@Test
	public void testVisionContextWithBase64Content() {
		String base64 = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
		reactor.keyValue.put("visionContext", base64);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(base64, result.getValue());
	}

	@Test
	public void testVisionContextWithURLs() {
		String urls = "https://example.com/page?param=value&other=123";
		reactor.keyValue.put("visionContext", urls);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(urls, result.getValue());
	}

	@Test
	public void testExecute_MultipleCallsWithDifferentContexts() {
		reactor.keyValue.put("visionContext", "Context 1");
		NounMetadata result1 = reactor.execute();
		assertEquals("Context 1", result1.getValue());

		reactor.keyValue.clear();
		reactor.keyValue.put("visionContext", "Context 2");
		NounMetadata result2 = reactor.execute();
		assertEquals("Context 2", result2.getValue());
	}

	@Test
	public void testVisionContextWithNumericString() {
		String numericString = "123456789";
		reactor.keyValue.put("visionContext", numericString);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(numericString, result.getValue());
		assertEquals(PixelDataType.CONST_STRING, result.getNounType());
	}

	@Test
	public void testVisionContextWithMixedContent() {
		String mixedContent = "Text with 123 numbers, @symbols, and special chars!";
		reactor.keyValue.put("visionContext", mixedContent);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(mixedContent, result.getValue());
	}

	@Test
	public void testVisionContextWithCSVFormat() {
		String csvContent = "header1,header2,header3\nvalue1,value2,value3\nvalue4,value5,value6";
		reactor.keyValue.put("visionContext", csvContent);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(csvContent, result.getValue());
	}


	@Test
	public void testReactorNounType() {
		reactor.keyValue.put("visionContext", "test");
		NounMetadata result = reactor.execute();
		assertEquals(PixelDataType.CONST_STRING, result.getNounType());
	}



	@Test
	public void testVisionContextWithSingleCharacter() {
		String singleChar = "A";
		reactor.keyValue.put("visionContext", singleChar);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(singleChar, result.getValue());
	}

	@Test
	public void testVisionContextWithNumberZero() {
		String zero = "0";
		reactor.keyValue.put("visionContext", zero);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(zero, result.getValue());
	}

	@Test
	public void testVisionContextWithSpaces() {
		String spaces = "   ";
		reactor.keyValue.put("visionContext", spaces);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(spaces, result.getValue());
	}

	@Test
	public void testVisionContextWithJSON() {
		String jsonContext = "{\"button\": {\"id\": \"submit\", \"text\": \"Submit Form\"}}";
		reactor.keyValue.put("visionContext", jsonContext);

		NounMetadata result = reactor.execute();

		assertNotNull(result);
		assertEquals(jsonContext, result.getValue());
	}
}

