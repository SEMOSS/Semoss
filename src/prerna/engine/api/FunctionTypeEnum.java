package prerna.engine.api;

import prerna.engine.impl.function.AWSTextractCustomEmbeddingsFunctionEngine;import prerna.engine.impl.function.AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine;
import prerna.engine.impl.function.GoogleOCRCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.ImageDescriptionFunctionEngine;
import prerna.engine.impl.function.LocalPythonCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.engine.impl.function.RESTFunctionEngine;
import prerna.engine.impl.function.OpenAITranscribeFunctionEngine;

/**
 * Enumeration defining the available function engine types in the SEMOSS platform.
 * 
 * <p>This enum provides a registry of all supported function engines, including both
 * standard function engines for general-purpose operations and specialized custom
 * embedding engines for advanced document processing and vector database integration.
 * Each enum value maps a function type name to its corresponding implementation class.</p>
 * 
 * <p>Function engines are categorized into:</p>
 * <ul>
 *   <li><strong>Standard Function Engines:</strong> General-purpose engines for Python execution,
 *       REST API calls, and transcription services</li>
 *   <li><strong>Custom Embedding Engines:</strong> Specialized engines that combine document
 *       processing (OCR, text extraction) with embedding generation for vector databases</li>
 * </ul>
 * 
 * @see {@link IFunctionEngine} for the base function engine interface
 * @see {@link ICustomEmbeddingsFunctionEngine} for custom embeddings functionality
 * @author SEMOSS
 */
public enum FunctionTypeEnum {

	// Standard function engines for general-purpose operations
	
	/** Local Python execution engine for running Python scripts and functions */
	LOCAL_PYTHON("LOCAL_PYTHON", LocalPythonFunctionEngine.class.getName()),
	
	/** REST API function engine for making HTTP requests and consuming web services */
	REST("REST", RESTFunctionEngine.class.getName()),

	/** OpenAI transcription engine for converting audio to text using OpenAI's Whisper API */
	OPENAI_TRANSCRIBE("OPENAI_TRANSCRIBE", OpenAITranscribeFunctionEngine.class.getName()),
	
	// Specialized function engines for custom embeddings integration with vector databases
	
	/** AWS Textract engine for OCR text extraction with custom embedding generation */
	AWS_TEXTRACT_CUSTOM_EMBEDDINGS("AWS_TEXTRACT_CUSTOM_EMBEDDINGS",
			AWSTextractCustomEmbeddingsFunctionEngine.class.getName()),
	
	/** Azure Document Intelligence engine for document processing with custom embeddings */
	AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS("AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS",
			AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine.class.getName()),
	
	/** Google OCR engine for optical character recognition with custom embedding generation */
	GOOGLE_OCR_CUSTOM_EMBEDDINGS("GOOGLE_OCR_CUSTOM_EMBEDDINGS",
			GoogleOCRCustomEmbeddingsFunctionEngine.class.getName()),
	
	/** Image description engine for generating textual descriptions of images */
	IMAGE_DESCRIPTION("IMAGE_DESCRIPTION", ImageDescriptionFunctionEngine.class.getName()),
	
	/** Local Python engine specialized for custom embedding generation and vector operations */
	LOCAL_PYTHON_CUSTOM_EMBEDDINGS("LOCAL_PYTHON_CUSTOM_EMBEDDINGS",
			LocalPythonCustomEmbeddingsFunctionEngine.class.getName()),;

	/** The human-readable name identifier for this function type */
	private String functionName;
	
	/** The fully qualified class name of the implementing function engine */
	private String functionClass;

	/**
	 * Constructs a function type enum with the specified name and implementation class.
	 * 
	 * @param functionName The human-readable identifier for this function type
	 * @param functionClass The fully qualified class name of the implementation
	 */
	FunctionTypeEnum(String functionName, String functionClass) {
		this.functionName = functionName;
		this.functionClass = functionClass;
	}

	/**
	 * Gets the human-readable name identifier for this function type.
	 * 
	 * @return The function type name used for identification and configuration
	 */
	public String getFunctionName() {
		return functionName;
	}

	/**
	 * Gets the fully qualified class name of the implementing function engine.
	 * 
	 * @return The complete class path for the function engine implementation
	 */
	public String getFunctionClass() {
		return functionClass;
	}

	/**
	 * Retrieves the function type enum that matches the specified name.
	 * 
	 * <p>This method performs a case-insensitive search through all available
	 * function types to find the one that matches the provided name. This is
	 * commonly used for configuration parsing and dynamic engine selection.</p>
	 * 
	 * @param name The function type name to search for (case-insensitive)
	 * @return The matching {@link FunctionTypeEnum} instance
	 * @throws IllegalArgumentException If no function type matches the provided name
	 */
	public static FunctionTypeEnum getEnumFromName(String name) {
		FunctionTypeEnum[] allValues = values();
		for (FunctionTypeEnum v : allValues) {
			if (v.getFunctionName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
