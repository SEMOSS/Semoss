package prerna.engine.api;

import prerna.engine.impl.model.BedrockEngine;
import prerna.engine.impl.model.EmbeddedModelEngine;
import prerna.engine.impl.model.FastChatProcessModel;
import prerna.engine.impl.model.OpenAiEngine;
import prerna.engine.impl.model.TextGenerationEngine;
import prerna.engine.impl.model.VertexEngine;
import prerna.engine.impl.remotesemoss.RemoteModelEngine;

public enum ModelTypeEnum {

	EMBEDDED("EMBEDDED", EmbeddedModelEngine.class.getName()),
	FAST_CHAT("FAST_CHAT", FastChatProcessModel.class.getName()),
	OPEN_AI("OPEN_AI", OpenAiEngine.class.getName()),
	BEDROCK("BEDROCK", BedrockEngine.class.getName()),
	VERTEX("VERTEX", VertexEngine.class.getName()),
	TEXT_GENERATION("TEXT_GENERATION", TextGenerationEngine.class.getName()),
	REMOTE("REMOTE", RemoteModelEngine.class.getName());
	
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
