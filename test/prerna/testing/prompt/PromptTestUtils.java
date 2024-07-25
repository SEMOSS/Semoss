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
import prerna.reactor.prompt.CheckPromptTitleReactor;
import prerna.reactor.prompt.ListPromptReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;

public class PromptTestUtils {
	private static final String TITLE_INPUT = "title";
	private static final String CONTEXT_INPUT = "context";
	private static final String IS_PUBLIC_INPUT = "is_public";
	private static final String IS_FAVORITE_INPUT = "favorite";
	private static final String TAG_INPUT = "tags";
	private static final String INPUTS = "inputs";
	private static final String INPUT_TYPES = "inputTypes";
	
	
	/**
	 * ADD PROMPT UTILS 
	 * 
	 */
	
	public static void addPrompt(String title, String context, List<String> tags, boolean isFavorite, boolean isPublic) {
		NounMetadata nm = runAddPrompt(title, context, tags, isFavorite, isPublic);
		assertNotEquals(PixelDataType.ERROR, nm.getNounType());
		assertTrue((boolean) nm.getValue());

	}
	
	public static String addPromptError(String title, String context, List<String> tags, boolean isFavorite, boolean isPublic) {
		NounMetadata nm = runAddPrompt(title, context, tags, isFavorite, isPublic);
		return (String) nm.getValue();
	}
	
	private static NounMetadata runAddPrompt(String title, String context, List<String> tags, boolean isFavorite, boolean isPublic) {
		List<Map<String, Object>> promptInputList = new ArrayList<>();
		Map<String, Object> promptDetailMap = new HashMap<>();
		promptDetailMap.put(TITLE_INPUT, title);
		promptDetailMap.put(CONTEXT_INPUT, context);
		promptDetailMap.put(IS_PUBLIC_INPUT, isPublic);
		promptDetailMap.put(IS_FAVORITE_INPUT, isFavorite);
		promptDetailMap.put(TAG_INPUT, tags);
		
		List<Map<String, Object>> promoptInputMap = makeInputMap(context);
		promptDetailMap.put(INPUTS, promoptInputMap);
		
		Map<String, Map<String, Object>> inputVaraibles = makeVaraiblesMap(promoptInputMap);
		promptDetailMap.put(INPUT_TYPES, inputVaraibles);
		promptInputList.add(promptDetailMap);
		
		String addPromptPixel = ApiSemossTestUtils.buildPixelCall(AddPromptReactor.class, "map",  promptInputList);
		System.out.println(addPromptPixel);
		NounMetadata nm = ApiSemossTestUtils.processRawPixel(addPromptPixel);
		return nm;
	}
	
	private static Map<String, Map<String, Object>> makeVaraiblesMap(List<Map<String, Object>> promoptInputMap) {
		Map<String, Map<String, Object>> retMap = new HashMap<>();
		for(Map<String, Object> inputDetails: promoptInputMap) {
			if(inputDetails.get("type") == "TOKEN_TYPE_INPUT") {
				String inputIndex = String.valueOf(inputDetails.get("index"));
				Map<String, Object> innerMap = new HashMap<>();
				innerMap.put("meta", "TEst");
				innerMap.put("type", "TOKEN_TYPE_INPUT");
				retMap.put(inputIndex, innerMap);	
			}
		}
		return retMap;
	}

	private static List<Map<String, Object>> makeInputMap(String context) {
		List<Map<String, Object>> retList = new ArrayList<Map<String, Object>>();
		String[] words = context.split(" ");
		Integer index = 0;
		String type = null;
		for(String word: words) {
			Map<String, Object> innerList = new HashMap<>();
			type = word.contains("{{") ? "TOKEN_TYPE_INPUT" :"TOKEN_TYPE_TEXT";
			word = word.contains("{{") ? word.substring(2, word.length()-2) : word;
			innerList.put("index", index);
			innerList.put("key", word);
			innerList.put("display", word);
			innerList.put("type", type);
			innerList.put("is_hidden_phrase_input_token", false);
			innerList.put("linked_input_token", "undefined");
			retList.add(innerList);
			index+=1;
		}
		return retList;
	}
	
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
