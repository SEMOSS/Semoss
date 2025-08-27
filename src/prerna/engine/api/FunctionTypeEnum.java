package prerna.engine.api;

import prerna.engine.impl.function.AWSTextractCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.AWSTranscribeFunctionEngine;
import prerna.engine.impl.function.AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine;
import prerna.engine.impl.function.GoogleOCRCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.ImageDescriptionFunctionEngine;
import prerna.engine.impl.function.LocalPythonCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.engine.impl.function.RESTFunctionEngine;

public enum FunctionTypeEnum {

	// normal function engines
	AWS_TRANSCRIBE("AWS_TRANSCRIBE", AWSTranscribeFunctionEngine.class.getName()),
	LOCAL_PYTHON("LOCAL_PYTHON", LocalPythonFunctionEngine.class.getName()),
	REST("REST", RESTFunctionEngine.class.getName()),
	
	// special function engines for custom embeddings w/ vector databases
	AWS_TEXTRACT_CUSTOM_EMBEDDINGS("AWS_TEXTRACT_CUSTOM_EMBEDDINGS", AWSTextractCustomEmbeddingsFunctionEngine.class.getName()),
	AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS("AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS", AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine.class.getName()),
	GOOGLE_OCR_CUSTOM_EMBEDDINGS("GOOGLE_OCR_CUSTOM_EMBEDDINGS", GoogleOCRCustomEmbeddingsFunctionEngine.class.getName()),
	IMAGE_DESCRIPTION("IMAGE_DESCRIPTION", ImageDescriptionFunctionEngine.class.getName()),
	LOCAL_PYTHON_CUSTOM_EMBEDDINGS("LOCAL_PYTHON_CUSTOM_EMBEDDINGS", LocalPythonCustomEmbeddingsFunctionEngine.class.getName()),
	;
	
	private String functionName;
	private String functionClass;
	
	FunctionTypeEnum(String functionName, String functionClass) {
		this.functionName = functionName;
		this.functionClass = functionClass;
	}
	
	public String getFunctionName() {
		return functionName;
	}

	public String getFunctionClass() {
		return functionClass;
	}

	/**
	 * 
	 * @param name
	 * @return
	 */
	public static FunctionTypeEnum getEnumFromName(String name) {
		FunctionTypeEnum[] allValues = values();
		for(FunctionTypeEnum v : allValues) {
			if(v.getFunctionName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
