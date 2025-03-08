package prerna.sablecc2.om.nounmeta;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;

public class GuardrailNounMetadata extends NounMetadata {
	
	public static final String PASS_KEY ="pass";
	public static final String RETURN_PROMPT_KEY = "returnPrompt";
	public static final String FULL_DETAILS_KEY = "fullDetails";
	
	/**
	 * Default constructor for preset nouns
	 */
	public GuardrailNounMetadata(boolean pass, String returnPrompt, Object details) {
		Map<String, Object> guardrailMap = new HashMap<>();
		guardrailMap.put(PASS_KEY, pass);
		guardrailMap.put(RETURN_PROMPT_KEY, returnPrompt);
		guardrailMap.put(FULL_DETAILS_KEY, details);
		this.value = guardrailMap;
		this.noun = PixelDataType.MAP;
	}
	
}
