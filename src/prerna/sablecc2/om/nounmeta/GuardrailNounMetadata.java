package prerna.sablecc2.om.nounmeta;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

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
		this.opType.add(PixelOperationType.GUARDRAIL);
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean isPass() {
		return (boolean) ((Map<String, Object>)this.value).get(PASS_KEY);
	}
	
	/**
	 * 
	 * @return
	 */
	public String getReturnPrompt() {
		return (String) ((Map<String, Object>)this.value).get(RETURN_PROMPT_KEY);
	}
	
	/**
	 * 
	 */
	public Object getFullDetails() {
		return ((Map<String, Object>)this.value).get(FULL_DETAILS_KEY);
	}
}
