package prerna.engine.api;

public enum ModelTypeEnum {

	EMBEDDED("EMBEDDED","prerna.engine.impl.model.EmbeddedModelEngine"),
	FAST_CHAT("FAST_CHAT","prerna.engine.impl.model.FastChatProcessModel"),
	OPEN_AI("OPEN_AI","prerna.engine.impl.model.OpenAiEngine"),
	BEDROCK("BEDROCK","prerna.engine.impl.model.BedrockEngine"),
	TEXT_GENERATION("TEXT_GENERATION","prerna.engine.impl.model.TextGenerationEngine"),
	REMOTE("REMOTE","prerna.engine.impl.remotesemoss.RemoteModelEngine");
	
	private String modelName;
	private String modelClass;
	
	ModelTypeEnum(String storageName, String storageClass) {
		this.modelName = storageName;
		this.modelClass = storageClass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getModelClass() {
		return this.modelClass;
	}
	
	/**
	 * 
	 * @return
	 */
	public String getModelName() {
		return this.modelName;
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	public static ModelTypeEnum getEnumFromName(String name) {
		ModelTypeEnum[] allValues = values();
		for(ModelTypeEnum v : allValues) {
			if(v.getModelName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
