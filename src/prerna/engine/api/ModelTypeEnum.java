package prerna.engine.api;

import prerna.engine.impl.model.BedrockEngine;
import prerna.engine.impl.model.EmbeddedModelEngine;
import prerna.engine.impl.model.NEREngine;
import prerna.engine.impl.model.OpenAiEngine;
import prerna.engine.impl.model.TextEmbeddingsEngine;
import prerna.engine.impl.model.TextGenerationEngine;
import prerna.engine.impl.model.VertexEngine;
import prerna.engine.impl.remotesemoss.RemoteModelEngine;
import prerna.engine.impl.model.KServeVisionEngine;
import prerna.engine.impl.model.KServeImageEmbedEngine;
import prerna.engine.impl.model.KServeImageEngine;
import prerna.engine.impl.model.KServeTTSEngine;

/**
 * Enumeration defining all available AI/ML model engine types in the SEMOSS platform.
 * 
 * <p>This enum provides a comprehensive registry of model engines that support various
 * AI and machine learning capabilities including natural language processing, computer
 * vision, text-to-speech, embeddings, and more. Each enum value maps a model type
 * identifier to its corresponding implementation class.</p>
 * 
 * <p>Supported model categories include:</p>
 * <ul>
 *   <li><strong>Cloud AI Services:</strong> Amazon Bedrock, Google Vertex AI, OpenAI</li>
 *   <li><strong>Text Processing:</strong> Text generation, embeddings, named entity recognition</li>
 *   <li><strong>Computer Vision:</strong> Image analysis, vision understanding, image embeddings</li>
 *   <li><strong>Speech Processing:</strong> Text-to-speech conversion</li>
 *   <li><strong>Embedded Models:</strong> Local model execution</li>
 *   <li><strong>Remote Execution:</strong> Distributed model serving via KServe or remote SEMOSS</li>
 * </ul>
 * 
 * @see {@link IModelEngine} for the base model engine interface
 * @see {@link BedrockEngine} for Amazon Bedrock integration
 * @see {@link OpenAiEngine} for OpenAI API integration
 * @see {@link VertexEngine} for Google Vertex AI integration
 * @author SEMOSS
 */
public enum ModelTypeEnum {

	/** Amazon Bedrock foundation models for text generation and conversation */
	BEDROCK("BEDROCK", BedrockEngine.class.getName()),
	
	/** Embedded models running locally within the SEMOSS platform */
	EMBEDDED("EMBEDDED", EmbeddedModelEngine.class.getName()),
	
	/** Named Entity Recognition models for identifying entities in text */
	NER("NER", NEREngine.class.getName()),
	
	/** KServe-hosted computer vision models for image analysis and understanding */
	KSERVE_VISION("KSERVE_VISION", KServeVisionEngine.class.getName()),
	
	/** KServe-hosted models for generating embeddings from images */
	KSERVE_IMAGE_EMBED("KSERVE_IMAGE_EMBED", KServeImageEmbedEngine.class.getName()),
	
	/** KServe-hosted image processing and generation models */
	KSERVE_IMAGE("KSERVE_IMAGE", KServeImageEngine.class.getName()),
	
	/** KServe-hosted text-to-speech synthesis models */
	KSERVE_TTS("KSERVE_TTS", KServeTTSEngine.class.getName()),
	
	/** OpenAI API models including GPT, embeddings, and other AI services */
	OPEN_AI("OPEN_AI", OpenAiEngine.class.getName()),
	
	/** Remote SEMOSS model engines for distributed model execution */
	REMOTE("REMOTE", RemoteModelEngine.class.getName()),
	
	/** Text embedding models for converting text to vector representations */
	TEXT_EMBEDDINGS("TEXT_EMBEDDINGS", TextEmbeddingsEngine.class.getName()),
	
	/** Text generation models for creating human-like text content */
	TEXT_GENERATION("TEXT_GENERATION", TextGenerationEngine.class.getName()),
	
	/** Google Vertex AI models for various AI and ML tasks */
	VERTEX("VERTEX", VertexEngine.class.getName()),
	;

	/** The human-readable name identifier for this model type */
	private String modelName;
	
	/** The fully qualified class name of the implementing model engine */
	private String modelClass;
	
	/**
	 * Constructs a model type enum with the specified name and implementation class.
	 * 
	 * @param modelName The human-readable identifier for this model type
	 * @param modelClass The fully qualified class name of the implementation
	 */
	ModelTypeEnum(String modelName, String modelClass) {
		this.modelName = modelName;
		this.modelClass = modelClass;
	}
	
	/**
	 * Gets the fully qualified class name of the implementing model engine.
	 * 
	 * @return The complete class path for the model engine implementation
	 */
	public String getModelClass() {
		return this.modelClass;
	}
	
	/**
	 * Gets the human-readable name identifier for this model type.
	 * 
	 * @return The model type name used for identification and configuration
	 */
	public String getModelName() {
		return this.modelName;
	}
	
	/**
	 * Retrieves the model type enum that matches the specified name.
	 * 
	 * <p>This method performs a case-insensitive search through all available
	 * model types to find the one that matches the provided name. This is
	 * commonly used for configuration parsing and dynamic engine selection.</p>
	 * 
	 * @param name The model type name to search for (case-insensitive)
	 * @return The matching {@link ModelTypeEnum} instance
	 * @throws IllegalArgumentException If no model type matches the provided name
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
