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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;

public class UpdatePromptReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void updateOnePromptTest() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		String intent = "Test Prompt";
		
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		// Capture the prompt ID from addPrompt
		String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotEquals(null, promptId);
		System.out.println("Created prompt with ID: " + promptId);

		NounMetadata listPrompts = PromptTestUtils.listPrompts();
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
		
		title = "Updated Test Title";
		context = "Updated Context {{Location}}";
		intent = "Updated Test intent";
		PromptTestUtils.updatePrompt(promptId, title, context, intent, tags, false, null);
		
		// Verify the first update was applied
		Map<String, Object> updatedPrompt1 = PromptTestUtils.getPrompt(promptId);
		assertEquals(title, updatedPrompt1.get("title"));
		assertEquals(context, updatedPrompt1.get("context"));
		assertEquals(intent, updatedPrompt1.get("intent"));
		
		title = "Updated Test Title2";
		context = "Updated Context {{Location}}2";
		intent = "Updated Test intent2";
		PromptTestUtils.updatePrompt(promptId, title, context, intent, tags, false, null);
		
		// Verify the second update was applied
		Map<String, Object> updatedPrompt2 = PromptTestUtils.getPrompt(promptId);
		assertEquals(title, updatedPrompt2.get("title"));
		assertEquals(context, updatedPrompt2.get("context"));
		assertEquals(intent, updatedPrompt2.get("intent"));
	}
	
	
	@Test
	public void updateOnePromptTestTwo() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		String intent = "Test Prompt";
		
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		// Capture the prompt ID from addPrompt
		String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotEquals(null, promptId);
		System.out.println("Created prompt with ID: " + promptId);

		NounMetadata listPrompts = PromptTestUtils.listPrompts();
		assertNotEquals(PixelDataType.ERROR, listPrompts.getValue());
		
		title = "Updated Test Title";
		context = "Updated Context {{Location}}";
		intent = "Updated Test intent";
		PromptTestUtils.updatePrompt(promptId, title, context, intent, null, false, null);
		
		// Verify the first update was applied
		Map<String, Object> updatedPrompt1 = PromptTestUtils.getPrompt(promptId);
		assertEquals(title, updatedPrompt1.get("title"));
		assertEquals(context, updatedPrompt1.get("context"));
		
		title = "Updated Test Title2";
		context = "Updated Context {{Location}}2";
		intent = "Updated Test intent2";
		tags = Arrays.asList("Parth");
		PromptTestUtils.updatePrompt(promptId, title, context, intent, tags, false, null);
		
		// Verify the second update was applied
		Map<String, Object> updatedPrompt2 = PromptTestUtils.getPrompt(promptId);
		assertEquals(title, updatedPrompt2.get("title"));
		assertEquals(context, updatedPrompt2.get("context"));
	}

	@Test
	public void updatePromptWithNullFieldTest() {
		// Create a prompt with all fields populated
		String title = "Initial Title";
		String context = "Initial context {{question}}";
		String intent = "Initial Intent";
		List<String> tags = Arrays.asList("tag1", "tag2");
		
		String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotEquals(null, promptId);
		
		// Verify initial prompt has intent
		Map<String, Object> initialPrompt = PromptTestUtils.getPrompt(promptId);
		assertEquals(intent, initialPrompt.get("intent"));
		
		// Update the prompt and set intent to null
		String updatedTitle = "Updated Title";
		String updatedContext = "Updated context {{answer}}";
		PromptTestUtils.updatePrompt(promptId, updatedTitle, updatedContext, null, tags, false, null);
		
		// Verify the updated prompt
		Map<String, Object> updatedPrompt = PromptTestUtils.getPrompt(promptId);
		assertEquals(updatedTitle, updatedPrompt.get("title"));
		assertEquals(updatedContext, updatedPrompt.get("context"));
		
		// Verify that intent is either null or not the string "null"
		Object intentValue = updatedPrompt.get("intent");
		assertTrue(intentValue == null || !intentValue.equals("null"), 
			"Intent should be null or not present, not the string 'null'");
		
		// More explicit check: if the key exists, it should be null, not the string "null"
		if (updatedPrompt.containsKey("intent")) {
			assertNull(intentValue, "Intent value should be null, not the string 'null'");
		}
	}
}
