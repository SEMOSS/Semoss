package prerna.engine.api;

import java.util.ArrayList;
import java.util.List;

public enum AWSComprehendInsightTypeEnum {

	ALL("All"), 
	DOMINANT_LANGUAGE("DominantLanguage"), 
	ENTITIES("Entities"), 
	SENTIMENT("Sentiment"),
	TARGETED_SENTIMENT("TargetedSentiment"), 
	PII_ENTITIES("PiiEntities"), 
	TOXIC_CONTENT("ToxicContent"),
	PROMPT_SAFETY("PromptSafety"), 
	KEY_PHRASES("KeyPhrases");

	private String insightType;

	private AWSComprehendInsightTypeEnum(String insightType) {
		this.insightType = insightType;
	}

	public String getInsightType() {
		return insightType;
	}

	public static AWSComprehendInsightTypeEnum getEnumFromInsightType(String insightType) {
		AWSComprehendInsightTypeEnum[] allValues = values();
		for (AWSComprehendInsightTypeEnum insightTypeEnum : allValues) {
			if (insightTypeEnum.getInsightType().equalsIgnoreCase(insightType)) {
				return insightTypeEnum;
			}
		}
		throw new IllegalArgumentException("Invalid input for insight type: " + insightType);
	}

	public static List<String> getAllValidInsightTypes() {
		List<String> insightTypes = new ArrayList<String>();
		AWSComprehendInsightTypeEnum[] allValues = values();
		for (AWSComprehendInsightTypeEnum insightTypeEnum : allValues) {
			if (insightTypeEnum != AWSComprehendInsightTypeEnum.ALL)
				insightTypes.add(insightTypeEnum.getInsightType());
		}
		return insightTypes;
	}
}
