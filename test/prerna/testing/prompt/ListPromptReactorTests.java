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
package prerna.testing.prompt;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.prompt.ListPromptReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class ListPromptReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void testListPromptUtils() {
		String pixel = ApiSemossTestUtils.buildPixelCall(ListPromptReactor.class);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotEquals(PixelDataType.ERROR, nm.getValue());
	}
	
	@Test
	public void listPromptsWithTagFilterTest() {
			
		String title = "Test-Title";
		String context = "Translate {{question}}";
		String intent = "Test Prompt";
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		
		String promptId1 = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotEquals(null, promptId1);
		
		// Changing vars for prompt 2 
		title = "Test-Title-2";
		context = "Translate the {{question}} int {{language}}";
		tags = Arrays.asList("World", "Travel");
		intent = "Test Prompt 2";
		String promptId2 = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotEquals(null, promptId2);
		
		List<String> metaTagsFilters = Arrays.asList("World");
		NounMetadata listPrompts = PromptTestUtils.listPrompts(metaTagsFilters);
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
		
	}
	
	@Test
	public void testListPromptsWithLimit() {
		// Create multiple prompts
		for (int i = 1; i <= 5; i++) {
			String title = "Limit-Test-Title-" + i;
			String context = "Test context " + i;
			String intent = "Test Intent " + i;
			List<String> tags = Arrays.asList("LimitTest");
			String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
			assertNotEquals(null, promptId);
		}
		
		// Test with limit = 3
		String pixel = ApiSemossTestUtils.buildPixelCall(
			ListPromptReactor.class,
			ReactorKeysEnum.LIMIT.getKey(), "3"
		);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotEquals(PixelDataType.ERROR, nm.getValue());
		
		List<Map<String, Object>> results = (List<Map<String, Object>>) nm.getValue();
		assertNotNull(results);
		assertTrue(results.size() <= 3, "Results should not exceed the limit of 3");
	}
	
	@Test
	public void testListPromptsWithOffset() {
		// Create multiple prompts
		for (int i = 1; i <= 5; i++) {
			String title = "Offset-Test-Title-" + i;
			String context = "Test context " + i;
			String intent = "Test Intent " + i;
			List<String> tags = Arrays.asList("OffsetTest");
			String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
			assertNotEquals(null, promptId);
		}
		
		// Test with offset = 2
		String pixel = ApiSemossTestUtils.buildPixelCall(
			ListPromptReactor.class,
			ReactorKeysEnum.OFFSET.getKey(), "2"
		);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotEquals(PixelDataType.ERROR, nm.getValue());
		assertNotNull(nm.getValue());
	}
	
	@Test
	public void testListPromptsWithLimitAndOffset() {
		// Create multiple prompts
		for (int i = 1; i <= 10; i++) {
			String title = "Pagination-Test-Title-" + i;
			String context = "Test context " + i;
			String intent = "Test Intent " + i;
			List<String> tags = Arrays.asList("PaginationTest");
			String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
			assertNotEquals(null, promptId);
		}
		
		// Test with limit = 3 and offset = 2
		String pixel = ApiSemossTestUtils.buildPixelCall(
			ListPromptReactor.class,
			ReactorKeysEnum.LIMIT.getKey(), "3",
			ReactorKeysEnum.OFFSET.getKey(), "2"
		);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotEquals(PixelDataType.ERROR, nm.getValue());
		
		List<Map<String, Object>> results = (List<Map<String, Object>>) nm.getValue();
		assertNotNull(results);
		assertTrue(results.size() <= 3, "Results should not exceed the limit of 3");
	}
	
	@Test
	public void testListPromptsWithMultipleMetaFilters() {
		// Create prompts with various tags
		String title1 = "Multi-Filter-Test-1";
		String context1 = "Context 1";
		String intent1 = "Intent 1";
		List<String> tags1 = Arrays.asList("Science", "Education", "Research");
		String promptId1 = PromptTestUtils.addPrompt(title1, context1, intent1, tags1, false, null);
		assertNotEquals(null, promptId1);
		
		String title2 = "Multi-Filter-Test-2";
		String context2 = "Context 2";
		String intent2 = "Intent 2";
		List<String> tags2 = Arrays.asList("Science", "Technology");
		String promptId2 = PromptTestUtils.addPrompt(title2, context2, intent2, tags2, false, null);
		assertNotEquals(null, promptId2);
		
		String title3 = "Multi-Filter-Test-3";
		String context3 = "Context 3";
		String intent3 = "Intent 3";
		List<String> tags3 = Arrays.asList("Education", "History");
		String promptId3 = PromptTestUtils.addPrompt(title3, context3, intent3, tags3, false, null);
		assertNotEquals(null, promptId3);
		
		// Filter by multiple tags - should match prompts with Science OR Education
		List<String> metaTagsFilters = Arrays.asList("Science", "Education");
		NounMetadata listPrompts = PromptTestUtils.listPrompts(metaTagsFilters);
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
		
		List<Map<String, Object>> results = (List<Map<String, Object>>) listPrompts.getValue();
		assertNotNull(results);
		assertTrue(results.size() >= 2, "Should return at least the prompts matching the filter tags");
	}
	
	@Test
	public void testListPromptsWithNoMatchingMetaFilters() {
		// Create prompts with specific tags
		String title = "NoMatch-Test-Title";
		String context = "Test context";
		String intent = "Test Intent";
		List<String> tags = Arrays.asList("UniqueTag123");
		String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotEquals(null, promptId);
		
		// Filter by a tag that doesn't exist
		List<String> metaTagsFilters = Arrays.asList("NonExistentTag999");
		NounMetadata listPrompts = PromptTestUtils.listPrompts(metaTagsFilters);
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
		
		// The result might be empty or contain only prompts without this tag
		List<Map<String, Object>> results = (List<Map<String, Object>>) listPrompts.getValue();
		assertNotNull(results);
	}
	
	@Test
	public void testListPromptsWithNullInputs() {
		// Test with null meta filters (should return all prompts)
		NounMetadata listPrompts = PromptTestUtils.listPrompts(null);
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
		assertNotNull(listPrompts.getValue());
	}
	
	@Test
	public void testListPromptsWithEmptyMetaFilters() {
		// Test with empty meta filters list (should return all prompts)
		NounMetadata listPrompts = PromptTestUtils.listPrompts(Arrays.asList());
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
		assertNotNull(listPrompts.getValue());
	}
}
