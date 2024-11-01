package prerna.engine.api;

import prerna.engine.impl.model.BedrockEngine;
import prerna.engine.impl.model.EmbeddedModelEngine;
import prerna.engine.impl.model.NamedEntityRecognitionEngine;
import prerna.engine.impl.model.NEREngine;
import prerna.engine.impl.model.OpenAiEngine;
import prerna.engine.impl.model.TextEmbeddingsEngine;
import prerna.engine.impl.model.TextGenerationEngine;
import prerna.engine.impl.model.VertexEngine;
import prerna.engine.impl.remotesemoss.RemoteModelEngine;


public enum ModelTypeEnum {

	BEDROCK("BEDROCK", BedrockEngine.class.getName()),
	EMBEDDED("EMBEDDED", EmbeddedModelEngine.class.getName()),
	// FAST_CHAT("FAST_CHAT", FastChatProcessModel.class.getName()),
	// Remove this
	NAMED_ENTITY_RECOGNITION("NAMED_ENTITY_RECOGNITION", NamedEntityRecognitionEngine.class.getName()),
	NER("NER", NEREngine.class.getName()),
	OPEN_AI("OPEN_AI", OpenAiEngine.class.getName()),
	// Can I remove this??
	REMOTE("REMOTE", RemoteModelEngine.class.getName()),
	TEXT_EMBEDDINGS("TEXT_EMBEDDINGS", TextEmbeddingsEngine.class.getName()),
	TEXT_GENERATION("TEXT_GENERATION", TextGenerationEngine.class.getName()),
	VERTEX("VERTEX", VertexEngine.class.getName()),
	;

	private String modelName;
	private String modelClass;
	
	ModelTypeEnum(String modelName, String modelClass) {
		this.modelName = modelName;
		this.modelClass = modelClass;
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
