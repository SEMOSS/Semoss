package prerna.testing.prompt;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.prompt.AddPromptReactor;
import prerna.reactor.prompt.UpdatePromptReactor;
import prerna.reactor.prompt.CheckPromptTitleReactor;
import prerna.reactor.prompt.DeletePromptReactor;
import prerna.reactor.prompt.ListPromptReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;

public class PromptTestUtils {
	private static final String TITLE_INPUT = "title";
	private static final String CONTEXT_INPUT = "context";
	private static final String INTENT_INPUT = "intent";
	private static final String IS_PUBLIC_INPUT = "is_public";
	private static final String IS_FAVORITE_INPUT = "favorite";
	private static final String TAG_INPUT = "tags";
	private static final String INPUTS = "inputs";
	private static final String INPUT_TYPES = "inputTypes";
	private static final String ID = "id";
	
	
	
	/**
	 * ADD PROMPT UTILS 
	 * 
	 */
	
	public static void addPrompt(String title, String context, String intent, List<String> tags) {
		NounMetadata nm = runAddPrompt(title, context, intent, tags);
		assertNotEquals(PixelDataType.ERROR, nm.getNounType());
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
}
