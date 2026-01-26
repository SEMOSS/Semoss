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
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.reactor.prompt.AddPromptReactor;
import prerna.reactor.prompt.UpdatePromptReactor;
import prerna.reactor.prompt.CheckPromptTitleReactor;
import prerna.reactor.prompt.DeletePromptReactor;
import prerna.reactor.prompt.GetPromptReactor;
import prerna.reactor.prompt.GetPromptMetaValuesReactor;
import prerna.reactor.prompt.ListPromptReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.ApiSemossTestInsightUtils;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import java.util.Collection;
import java.util.Set;

public class PromptTestUtils {
	private static final String TITLE_INPUT = "title";
	private static final String CONTEXT_INPUT = "context";
	private static final String INTENT_INPUT = "intent";
	private static final String TAG_INPUT = "tags";
	private static final String ID = "id";
	
	
	
	/**
	 * ADD PROMPT UTILS 
	 * 
	 */
	
	public static void addPrompt(String title, String context, String intent, List<String> tags) {
		NounMetadata nm = runAddPrompt(title, context, intent, tags);
		if (nm.getNounType() == PixelDataType.ERROR) {
			fail("AddPrompt failed with error: " + nm.getValue());
		}
//		assertTrue((boolean) nm.getValue());

	}
	
	public static String addPromptError(String title, String context, String intent, List<String> tags) {
		NounMetadata nm = runAddPrompt(title, context, intent, tags);
		return (String) nm.getValue();
	}
	
	private static NounMetadata runAddPrompt(String title, String context, String intent, List<String> tags) {
		List<Map<String, Object>> promptInputList = new ArrayList<>();
		Map<String, Object> promptDetailMap = new HashMap<>();
		promptDetailMap.put(TITLE_INPUT, title);
		promptDetailMap.put(CONTEXT_INPUT, context);
		promptDetailMap.put(INTENT_INPUT, intent);
		promptDetailMap.put(TAG_INPUT, tags);
		promptDetailMap.put("metaMap", new HashMap<>()); // Empty metaMap to avoid null pointer
		
//		List<Map<String, Object>> promoptInputMap = makeInputMap(context);
//		promptDetailMap.put(INPUTS, promoptInputMap);
//		
//		Map<String, Map<String, Object>> inputVaraibles = makeVaraiblesMap(promoptInputMap);
//		promptDetailMap.put(INPUT_TYPES, inputVaraibles);
		promptInputList.add(promptDetailMap);
		
		String addPromptPixel = ApiSemossTestUtils.buildPixelCall(AddPromptReactor.class, "map",  promptInputList);
		System.out.println(addPromptPixel);
		NounMetadata nm = ApiSemossTestUtils.processRawPixel(addPromptPixel);
		return nm;
	}
	
	/**
	 * UPDATE PROMPT UTILS
	 */
	
	public static void updatePrompt(String promptId, String title, String context, String intent, List<String> tags) {
		NounMetadata nm = runUpdatePrompt(promptId, title, context, intent, tags);
		assertNotEquals(PixelDataType.ERROR, nm.getNounType());
//		assertTrue((boolean) nm.getValue());

	}
	
	private static NounMetadata runUpdatePrompt(String promptId, String title, String context, String intent, List<String> tags) {
		List<Map<String, Object>> promptInputList = new ArrayList<>();
		Map<String, Object> promptDetailMap = new HashMap<>();
		promptDetailMap.put(ID, promptId);
		promptDetailMap.put(TITLE_INPUT, title);
		promptDetailMap.put(CONTEXT_INPUT, context);
		promptDetailMap.put(INTENT_INPUT, intent);
		promptDetailMap.put(TAG_INPUT, tags);
		promptDetailMap.put("metaMap", new HashMap<>()); // Empty metaMap to avoid null pointer

		promptInputList.add(promptDetailMap);
		
		String updatePromptPixel = ApiSemossTestUtils.buildPixelCall(UpdatePromptReactor.class, "map",  promptInputList);
		System.out.println(updatePromptPixel);
		NounMetadata nm = ApiSemossTestUtils.processRawPixel(updatePromptPixel);
		return nm;
	}
	
	/**
	 * DELETE PROMPT UTILS
	 */
	public static void deletePrompt(String promptId) {
		NounMetadata nm = runDeletePrompt(promptId);
		assertNotEquals(PixelDataType.ERROR, nm.getNounType());
		assertTrue((boolean) nm.getValue());

	}
	
	private static NounMetadata runDeletePrompt(String promptId) {
		String deletePromptPixel = ApiSemossTestUtils.buildPixelCall(DeletePromptReactor.class, "promptId",  promptId);
		System.out.println(deletePromptPixel);
		NounMetadata nm = ApiSemossTestUtils.processRawPixel(deletePromptPixel);
		return nm;
	}
	
//	private static Map<String, Map<String, Object>> makeVaraiblesMap(List<Map<String, Object>> promoptInputMap) {
//		Map<String, Map<String, Object>> retMap = new HashMap<>();
//		for(Map<String, Object> inputDetails: promoptInputMap) {
//			if(inputDetails.get("type") == "TOKEN_TYPE_INPUT") {
//				String inputIndex = String.valueOf(inputDetails.get("index"));
//				Map<String, Object> innerMap = new HashMap<>();
//				innerMap.put("meta", "TEst");
//				innerMap.put("type", "TOKEN_TYPE_INPUT");
//				retMap.put(inputIndex, innerMap);	
//			}
//		}
//		return retMap;
//	}
//
//	private static List<Map<String, Object>> makeInputMap(String context) {
//		List<Map<String, Object>> retList = new ArrayList<Map<String, Object>>();
//		String[] words = context.split(" ");
//		Integer index = 0;
//		String type = null;
//		for(String word: words) {
//			Map<String, Object> innerList = new HashMap<>();
//			type = word.contains("{{") ? "TOKEN_TYPE_INPUT" :"TOKEN_TYPE_TEXT";
//			word = word.contains("{{") ? word.substring(2, word.length()-2) : word;
//			innerList.put("index", index);
//			innerList.put("key", word);
//			innerList.put("display", word);
//			innerList.put("type", type);
//			innerList.put("is_hidden_phrase_input_token", false);
//			innerList.put("linked_input_token", "undefined");
//			retList.add(innerList);
//			index+=1;
//		}
//		return retList;
//	}
	
	/**
	 * LIST PROMPT UTILS 
	 */
	
	public static NounMetadata listPrompts() {
		return listPrompts(null);
	}
	
	public static NounMetadata listPrompts(List<String> metaTagFilterList) {
		List<Map<String, Object>> finalMetaMap = new ArrayList<>();
		String listPromptsPixel = null;
		if(metaTagFilterList != null && !metaTagFilterList.isEmpty()) {
			finalMetaMap.add(createMetaMap(metaTagFilterList));
			listPromptsPixel = ApiSemossTestUtils.buildPixelCall(ListPromptReactor.class, "metaFilters", finalMetaMap);
		} else {
			listPromptsPixel = ApiSemossTestUtils.buildPixelCall(ListPromptReactor.class);			
		}
		System.out.println(listPromptsPixel);
		NounMetadata nm = ApiSemossTestUtils.processPixel(listPromptsPixel);
		System.out.println(nm.getValue());
		return nm;
	}
	
	private static Map<String, Object> createMetaMap(List<String> metaTags) {
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("tag", metaTags);
		return retMap;
	}
	
	/**
	 * CheckPromptTitleReactor Utils
	 * 
	 */
	
	public static boolean checkPromptTitle(String title) {
		String checkPromptTitlePixel = ApiSemossTestUtils.buildPixelCall(CheckPromptTitleReactor.class, "promptTitle", title);
		System.out.println(checkPromptTitlePixel);
		NounMetadata nm = ApiSemossTestUtils.processPixel(checkPromptTitlePixel);
		return (boolean) nm.getValue();
	}
	
	/**
	 * CREATE USER UTILS WITH METADATA
	 */
	
	public static User createTestUser(String username, boolean isAdmin, Map<String, Collection<String>> metadata) {
		// Create access token
		AccessToken at = new AccessToken();
		at.setId(username);
		at.setName(username);
		at.setUsername(username);
		at.setEmail(username + "@test.com");
		at.setProvider(AuthProvider.NATIVE);
		
		// If metadata is provided, set it on the access token and register metakeys
		if (metadata != null && !metadata.isEmpty()) {
			at.setMeta(metadata);
			registerUserMetakeys(metadata);
		}
		
		// Register the user in security database (skip assertion if user already exists)
		boolean registered = SecurityUpdateUtils.registerUser(at.getId(), at.getName(), at.getEmail(), "Test123!",
				AuthProvider.NATIVE.getLabel(), "5555555555", "001", "US", isAdmin, false, false, null, null, null, null);
		// Note: registerUser may return false if user already exists, which is fine in test context
		
		// Create and return User object
		User user = new User();
		user.setPrimaryLogin(AuthProvider.NATIVE);
		user.setAccessToken(at);
		
		return user;
	}
	
	/**
	 * Sets the current user in the test insight with the specified metadata
	 * @param username The username
	 * @param email The user's email
	 * @param metadata The metadata to set on the user's access token
	 */
	public static void setUserWithMetadata(String username, String email, Map<String, Collection<String>> metadata) {
		User user = new User();
		AccessToken at = new AccessToken();
		at.setProvider(AuthProvider.NATIVE);
		at.setId(username);
		at.setEmail(email);
		at.setUsername(username);
		at.setName(username);
		
		// Set metadata on the access token
		if (metadata != null && !metadata.isEmpty()) {
			at.setMeta(metadata);
		}
		
		user.setAccessToken(at);
		user.setPrimaryLogin(AuthProvider.NATIVE);
		ApiSemossTestInsightUtils.getInsight().setUser(user);
	}
	
	private static void registerUserMetakeys(Map<String, Collection<String>> metadata) {
		// Register each metakey in the security database using updateMetakeyOptions
		List<Map<String, Object>> metakeyOptions = new ArrayList<>();
		int displayOrder = 1;
		for (String metakey : metadata.keySet()) {
			Map<String, Object> option = new HashMap<>();
			option.put("metakey", metakey);
			option.put("single_multi", "single");
			option.put("display_order", displayOrder++);
			option.put("display_options", metakey + "_display");
			option.put("display_values", "Test metakey for " + metakey);
			metakeyOptions.add(option);
		}
		
		if (!metakeyOptions.isEmpty()) {
			try {
				// Get existing metakeys and merge with new ones
				List<Map<String, Object>> existingOptions = SecurityUserUtils.getMetakeyOptions(null);
				Set<String> existingKeys = existingOptions.stream()
						.map(m -> (String) m.get("metakey"))
						.collect(java.util.stream.Collectors.toSet());
				
				// Only add new metakeys, keep existing ones
				for (Map<String, Object> option : metakeyOptions) {
					if (!existingKeys.contains(option.get("metakey"))) {
						existingOptions.add(option);
					}
				}
				
				SecurityUserUtils.updateMetakeyOptions(existingOptions);
			} catch (Exception e) {
				// Metakeys might already exist, which is fine
				System.out.println("Error registering metakeys: " + e.getMessage());
			}
		}
	}
	
	/**
	 * ADD PROMPT WITH GLOBAL AND METADATA UTILS
	 */
	
	public static void addPromptWithGlobal(String title, String context, String intent, 
	                                       List<String> tags, Boolean global, 
	                                       Map<String, Collection<String>> metaMap) {
		NounMetadata nm = runAddPromptWithGlobal(title, context, intent, tags, global, metaMap);
		if (nm.getNounType() == PixelDataType.ERROR) {
			fail("AddPromptWithGlobal failed with error: " + nm.getValue());
		}
	}
	
	public static String addPromptWithGlobalExpectError(String title, String context, String intent, 
	                                                    List<String> tags, Boolean global, 
	                                                    Map<String, Collection<String>> metaMap) {
		NounMetadata nm = runAddPromptWithGlobal(title, context, intent, tags, global, metaMap);
		return (String) nm.getValue();
	}
	
	private static NounMetadata runAddPromptWithGlobal(String title, String context, String intent, 
	                                                   List<String> tags, Boolean global, 
	                                                   Map<String, Collection<String>> metaMap) {
		List<Map<String, Object>> promptInputList = new ArrayList<>();
		Map<String, Object> promptDetailMap = new HashMap<>();
		promptDetailMap.put(TITLE_INPUT, title);
		promptDetailMap.put(CONTEXT_INPUT, context);
		promptDetailMap.put(INTENT_INPUT, intent);
		promptDetailMap.put(TAG_INPUT, tags);
		
		if (global != null) {
			promptDetailMap.put("global", global);
		}
		
		// Only add metaMap if it's not null and not empty
		// If null, don't include it at all (prompt will have no metadata restrictions)
		// If empty, include it as empty map to avoid null pointer in PromptUtils.addPrompt
		if (metaMap != null) {
			promptDetailMap.put("metaMap", metaMap);
		} else {
			// Add empty map to avoid NPE in PromptUtils.addPrompt line 251 (userSelectedMeta.remove("tags"))
			promptDetailMap.put("metaMap", new HashMap<>());
		}
		
		promptInputList.add(promptDetailMap);
		
		String addPromptPixel = ApiSemossTestUtils.buildPixelCall(AddPromptReactor.class, "map", promptInputList);
		System.out.println(addPromptPixel);
		NounMetadata nm = ApiSemossTestUtils.processRawPixel(addPromptPixel);
		return nm;
	}
	
	/**
	 * UPDATE PROMPT WITH GLOBAL AND METADATA UTILS
	 */
	
	public static void updatePromptWithGlobal(String promptId, String title, String context, 
	                                         String intent, List<String> tags, Boolean global, 
	                                         Map<String, Collection<String>> metaMap) {
		NounMetadata nm = runUpdatePromptWithGlobal(promptId, title, context, intent, tags, global, metaMap);
		assertNotEquals(PixelDataType.ERROR, nm.getNounType());
	}
	
	public static String updatePromptExpectError(String promptId, String title, String context, 
	                                            String intent, List<String> tags, Boolean global, 
	                                            Map<String, Collection<String>> metaMap) {
		NounMetadata nm = runUpdatePromptWithGlobal(promptId, title, context, intent, tags, global, metaMap);
		return (String) nm.getValue();
	}
	
	private static NounMetadata runUpdatePromptWithGlobal(String promptId, String title, String context, 
	                                                      String intent, List<String> tags, Boolean global, 
	                                                      Map<String, Collection<String>> metaMap) {
		List<Map<String, Object>> promptInputList = new ArrayList<>();
		Map<String, Object> promptDetailMap = new HashMap<>();
		promptDetailMap.put(ID, promptId);
		promptDetailMap.put(TITLE_INPUT, title);
		promptDetailMap.put(CONTEXT_INPUT, context);
		promptDetailMap.put(INTENT_INPUT, intent);
		promptDetailMap.put(TAG_INPUT, tags);
		
		if (global != null) {
			promptDetailMap.put("global", global);
		}
		
		// Always add metaMap, even if empty, to avoid null pointer in PromptUtils.editPrompt
		promptDetailMap.put("metaMap", metaMap != null ? metaMap : new HashMap<>());
		
		promptInputList.add(promptDetailMap);
		
		String updatePromptPixel = ApiSemossTestUtils.buildPixelCall(UpdatePromptReactor.class, "map", promptInputList);
		System.out.println(updatePromptPixel);
		NounMetadata nm = ApiSemossTestUtils.processRawPixel(updatePromptPixel);
		return nm;
	}
	
	/**
	 * DELETE PROMPT EXPECT ERROR UTILS
	 */
	
	public static String deletePromptExpectError(String promptId) {
		NounMetadata nm = runDeletePrompt(promptId);
		return (String) nm.getValue();
	}
	
	/**
	 * GET PROMPT UTILS
	 */
	
	@SuppressWarnings("unchecked")
	public static Map<String, Object> getPrompt(String promptId) {
		NounMetadata nm = runGetPrompt(promptId);
		assertNotEquals(PixelDataType.ERROR, nm.getNounType());
		return (Map<String, Object>) nm.getValue();
	}
	
	public static String getPromptExpectError(String promptId) {
		NounMetadata nm = runGetPrompt(promptId);
		System.out.println("DEBUG getPromptExpectError - NounType: " + nm.getNounType() + ", Value: " + nm.getValue());
		// If it's an ERROR type, the error message is in the value
		// If it's not an ERROR, we should return the toString of the value
		if (nm.getNounType() == PixelDataType.ERROR) {
			return (String) nm.getValue();
		}
		// Return null if no error
		return null;
	}
	
	private static NounMetadata runGetPrompt(String promptId) {
		String getPromptPixel = ApiSemossTestUtils.buildPixelCall(GetPromptReactor.class, "promptId", promptId);
		System.out.println(getPromptPixel);
		NounMetadata nm = ApiSemossTestUtils.processRawPixel(getPromptPixel);
		return nm;
	}
	
	/**
	 * GET PROMPT META VALUES UTILS
	 */
	
	@SuppressWarnings("unchecked")
	public static List<String> getPromptMetaValues(List<String> metaKeys) {
		NounMetadata nm = runGetPromptMetaValues(metaKeys);
		assertNotEquals(PixelDataType.ERROR, nm.getNounType());
		return (List<String>) nm.getValue();
	}
	
	public static String getPromptMetaValuesExpectError(List<String> metaKeys) {
		NounMetadata nm = runGetPromptMetaValues(metaKeys);
		System.out.println("DEBUG getPromptMetaValuesExpectError - NounType: " + nm.getNounType() + ", Value: " + nm.getValue());
		if (nm.getNounType() == PixelDataType.ERROR) {
			return (String) nm.getValue();
		}
		return null;
	}
	
	private static NounMetadata runGetPromptMetaValues(List<String> metaKeys) {
		String getMetaValuesPixel = ApiSemossTestUtils.buildPixelCall(GetPromptMetaValuesReactor.class, 
		                                                              "metakeys", metaKeys);
		System.out.println(getMetaValuesPixel);
		NounMetadata nm = ApiSemossTestUtils.processRawPixel(getMetaValuesPixel);
		return nm;
	}
}
