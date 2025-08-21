package prerna.engine.api;

import prerna.engine.impl.function.AWSTextractFunctionEngine;
import prerna.engine.impl.function.AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine;
import prerna.engine.impl.function.GoogleOCRFunctionEngine;
import prerna.engine.impl.function.ImageDescriptionFunctionEngine;
import prerna.engine.impl.function.LocalPythonCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.engine.impl.function.RESTFunctionEngine;
import prerna.engine.impl.function.AWSTranscribeFunctionEngine;

public enum FunctionTypeEnum {

	// normal function engines
	AWS_TEXTRACT("AWS_TEXTRACT", AWSTextractFunctionEngine.class.getName()),
	IMAGE_DESCRIPTION("IMAGE_DESCRIPTION", ImageDescriptionFunctionEngine.class.getName()),
	LOCAL_PYTHON("LOCAL_PYTHON", LocalPythonFunctionEngine.class.getName()),
	REST("REST", RESTFunctionEngine.class.getName()),
	
	// special function engines for custom embeddings w/ vector databases
	AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS("AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS", AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine.class.getName()),
	LOCAL_PYTHON_CUSTOM_EMBEDDINGS("LOCAL_PYTHON_CUSTOM_EMBEDDINGS", LocalPythonCustomEmbeddingsFunctionEngine.class.getName()),
	GOOGLE_OCR("GOOGLE_OCR", GoogleOCRFunctionEngine.class.getName()),
	AWS_Transcribe("AWS_Transcribe", AWSTranscribeFunctionEngine.class.getName()),
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
