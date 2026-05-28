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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUserUtils;

public class DeletePromptReactorTests extends AbstractBaseSemossApiTests {
	
	@Test
	@SuppressWarnings("unchecked")
	public void deletePromptTest() {
		String title = "Test-Title";
		String context = "Translate {{question}}";
		String intent = "Test Prompt";
		
		List<String> tags = Arrays.asList("World", "GAMING", "PLANTS"); 
		// Capture the prompt ID from addPrompt
		String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotEquals(null, promptId);
		System.out.println("Created prompt with ID: " + promptId);
		
		// Verify the prompt exists before deletion
		Map<String, Object> createdPrompt = PromptTestUtils.getPrompt(promptId);
		assertNotEquals(null, createdPrompt);
		assertEquals(title, createdPrompt.get("title"));
		
		// Delete the prompt using the ID we got from addPrompt
		String deletedPromptId = PromptTestUtils.deletePrompt(promptId);
		assertEquals(promptId, deletedPromptId);
		System.out.println("Deleted prompt ID: " + deletedPromptId);
		
		// Verify the prompt was actually deleted by checking it's not in the list
		NounMetadata listPrompts = PromptTestUtils.listPrompts();
		List<Map<String, Object>> prompts = (List<Map<String, Object>>) listPrompts.getValue();
		boolean promptStillExists = prompts.stream()
			.anyMatch(p -> promptId.equals(p.get("id")));
		assertFalse(promptStillExists, "Prompt should have been deleted");
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testRegularUserDeletesOwnPrompt() {
		// Create a regular user and add a prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user", "user@test.com", false);
		
		String title = "User Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotNull(promptId);
		
		// User should be able to delete their own prompt
		String deletedPromptId = PromptTestUtils.deletePrompt(promptId);
		assertEquals(promptId, deletedPromptId);
		
		// Verify prompt was deleted
		NounMetadata afterDelete = PromptTestUtils.listPrompts();
		List<Map<String, Object>> afterPrompts = (List<Map<String, Object>>) afterDelete.getValue();
		assertEquals(0, afterPrompts.size());
	}

	@Test
	public void testRegularUserCannotDeleteOthersPrompt() {
		// Create first user and add a prompt
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user1", "user1@test.com", false);
		
		String title = "User1 Prompt";
		String context = "Test context {{question}}";
		String intent = "Test intent";
		List<String> tags = Arrays.asList("test");
		
		String promptId = PromptTestUtils.addPrompt(title, context, intent, tags, false, null);
		assertNotNull(promptId);
		
		// Switch to second user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", "user2@test.com", false);
		
		// Second user should NOT be able to delete first user's prompt
		String errorMsg = PromptTestUtils.deletePromptExpectError(promptId);
		assertNotNull(errorMsg);
		assertTrue(errorMsg.contains("permission") || errorMsg.contains("does not have"));
	}
}
